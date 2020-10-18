#include <afina/concurrency/Executor.h>

namespace Afina {
namespace Concurrency {

Executor::Executor(std::size_t low_watermark, std::size_t high_watermark, std::size_t max_queue_size, int64_t idle_time) 
                : _low_watermark(low_watermark), _high_watermark(high_watermark), _max_queue_size(max_queue_size), _idle_time(idle_time){
    std::unique_lock<std::mutex> lock(mutex);
    state = State::kRun;
    _cur_running = 0;
    _existing_threads = low_watermark;
    for (std::size_t i = 0; i < _low_watermark; ++i){
        std::thread([this](){
            this->perform();
        }).detach();
    }
}

Executor::~Executor(){
    Stop(true);
}

void Executor::Stop(bool await){
    std::unique_lock<std::mutex> lock(mutex);
    if (state != State::kStopped){
        state = State::kStopping;
        if (!_existing_threads){
            state = State::kStopped;
        } else {
            empty_condition.notify_all();
            while (state != State::kStopped){
                _executor_stop.wait(lock);
            }
        }
    }
}

void Executor::perform(){
    std::unique_lock<std::mutex> lock(mutex);
    int64_t remaining_time = _idle_time;
    while (state == State::kRun){
        bool timeout = false;
        while (state == State::kRun && tasks.empty()){
            auto begin = std::chrono::steady_clock::now();
            auto wait_res = empty_condition.wait_until(lock, begin + std::chrono::milliseconds(remaining_time));
            if (wait_res == std::cv_status::timeout){
                timeout = true;
                break;
            } else {
                remaining_time -= std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
            }
        }
        if (timeout && _existing_threads > _low_watermark || tasks.empty()){
            break;
        }
        auto task(std::move(tasks.front()));
        tasks.pop();
        ++_cur_running;
        lock.unlock();
        task();
        lock.lock();
        --_cur_running;
    }
    --_existing_threads;
    if (!_existing_threads && state == State::kStopping){
        state = State::kStopped;
        _executor_stop.notify_all();
    } 
}

} // namespace Concurrency
} // namespace Afina
