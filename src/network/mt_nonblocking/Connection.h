#ifndef AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H

#include <cstring>
#include <string>
#include <vector>
#include <sys/epoll.h>
#include "protocol/Parser.h"
#include <afina/execute/Command.h>
#include <spdlog/logger.h>
#include <mutex>
#include <atomic>

#define MAX_OUTPUT_QUEUE_SIZE 100

namespace Afina {
namespace Network {
namespace MTnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage>& ps, std::shared_ptr<spdlog::logger>& pl)
     : _socket(s), _pStorage(ps), _pLogger(pl) {
        std::unique_lock<std::mutex> lock(_mutex);
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
        _is_alive.store(true, std::memory_order::memory_order_relaxed);
        _arg_remains = 0;
        _buff_offset = 0;
        _head_offset = 0;
        _eof.store(false, std::memory_order::memory_order_release);
        std::memset(_read_buffer, 0, 4096);
    }

    inline bool isAlive() const {
        return _is_alive.load(std::memory_order_acquire);
    }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class ServerImpl;
    friend class Worker;

    int _socket;
    std::atomic<bool> _is_alive;
    std::atomic<bool> _eof;
    struct epoll_event _event;
    std::vector<std::string> output;
    std::size_t _head_offset;

    std::shared_ptr<Afina::Storage> _pStorage;
    std::shared_ptr<spdlog::logger> _pLogger;
    char _read_buffer[4096];
    int _buff_offset;

    std::size_t _arg_remains;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;
    std::mutex _mutex;
};

} // namespace MTnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
