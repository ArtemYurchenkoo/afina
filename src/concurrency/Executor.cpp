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
    if (state == State::kRun){
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
    auto begin = std::chrono::steady_clock::now();
    bool running = false;
    while (state == State::kRun){
        std::unique_lock<std::mutex> lock(mutex);
        if (running){
            --_cur_running;
            begin = std::chrono::steady_clock::now();
            running = false;
        }
        while (tasks.empty() && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count() < _idle_time){
            empty_condition.wait(lock);
        }
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);
        if (elapsed_ms.count() >= _idle_time && _existing_threads > _low_watermark){
            break;
        }
        if (tasks.empty()){
            continue;
        }
        auto task(std::move(tasks.front()));
        tasks.pop();
        ++_cur_running;
        running = true;
        lock.unlock();
        task();
    }
    std::unique_lock<std::mutex> lock(mutex);
    --_existing_threads;
    if (!_existing_threads && state == State::kStopping){
        state = State::kStopped;
        _executor_stop.notify_all();
    } 
}

} // namespace Concurrency
} // namespace Afina
