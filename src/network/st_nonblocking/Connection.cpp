#include "Connection.h"

#include <unistd.h>

namespace Afina {
namespace Network {
namespace STnonblock {

// See Connection.h
void Connection::Start() { 
    _event.events |= EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;
    _pLogger->debug("Connection started on socket {}", _socket);
}

// See Connection.h
void Connection::OnError() { 
    _pLogger->error("Error on connection on socket {}", _socket);
    _is_alive = false;
}

// See Connection.h
void Connection::OnClose() { 
    _pLogger->debug("Connection closed on socket {}", _socket);
    _is_alive = false;
}

// See Connection.h
void Connection::DoRead() { 
    _pLogger->debug("Connection reading on socket {}", _socket);
    
    try {
        int readed_bytes = -1;
        while ((readed_bytes = read(_socket, _read_buffer + _buff_offset, sizeof(_read_buffer) - _buff_offset)) > 0) {
            _pLogger->debug("Got {} bytes from socket", readed_bytes);
            _buff_offset += readed_bytes;
            // Single block of data readed from the socket could trigger inside actions a multiple times,
            // for example:
            // - read#0: [<command1 start>]
            // - read#1: [<command1 end> <argument> <command2> <argument for command 2> <command3> ... ]
            while (_buff_offset > 0) {
                _pLogger->debug("Process {} bytes", _buff_offset);
                // There is no command yet
                if (!command_to_execute) {
                    std::size_t parsed = 0;
                    if (parser.Parse(_read_buffer, _buff_offset, parsed)) {
                        // There is no command to be launched, continue to parse input stream
                        // Here we are, current chunk finished some command, process it
                        _pLogger->debug("Found new command: {} in {} bytes", parser.Name(), parsed);
                        command_to_execute = parser.Build(_arg_remains);
                        if (_arg_remains > 0) {
                            _arg_remains += 2;
                        }
                    }

                    // Parsed might fail to consume any bytes from input stream. In real life that could happen,
                    // for example, because we are working with UTF-16 chars and only 1 byte left in stream
                    if (parsed == 0) {
                        break;
                    } else {
                        std::memmove(_read_buffer, _read_buffer + parsed, _buff_offset - parsed);
                        _buff_offset -= parsed;
                    }
                }

                // There is command, but we still wait for argument to arrive...
                if (command_to_execute && _arg_remains > 0) {
                    _pLogger->debug("Fill argument: {} bytes of {}", _buff_offset, _arg_remains);
                    // There is some parsed command, and now we are reading argument
                    std::size_t to_read = std::min(_arg_remains, std::size_t(_buff_offset));
                    argument_for_command.append(_read_buffer, to_read);

                    std::memmove(_read_buffer, _read_buffer + to_read, _buff_offset - to_read);
                    _arg_remains -= to_read;
                    _buff_offset -= to_read;
                }

                // There is command & argument - RUN!
                if (command_to_execute && _arg_remains == 0) {
                    _pLogger->debug("Start command execution");

                    std::string result;
                    if (argument_for_command.size()) {
                        argument_for_command.resize(argument_for_command.size() - 2);
                    }
                    command_to_execute->Execute(*_pStorage, argument_for_command, result);

                    result += "\r\n";
                    output.push_back(std::move(result));
                    _event.events |= EPOLLOUT;
                    if (output.size() >= MAX_OUTPUT_QUEUE_SIZE){
                        _event.events &= ~EPOLLIN;
                    }

                    // Prepare for the next command
                    command_to_execute.reset();
                    argument_for_command.resize(0);
                    parser.Reset();
                }
            } // while (readed_bytes)
        }
        if (readed_bytes == 0) {
            _pLogger->debug("Client closed connection on socket {}", _socket);
            _eof = true;
        } else {
            throw std::runtime_error(std::string(strerror(errno)));
        }
    } catch (std::runtime_error &ex) {
        if (errno != EAGAIN){
            _pLogger->error("Failed to process connection on descriptor {}: {}", _socket, ex.what());
            _is_alive = false;
        }
    }
}

// See Connection.h
void Connection::DoWrite() { 
    if  (!_is_alive){
        return;
    }
    _pLogger->debug("Connection writing on socket {}", _socket);

    int ret = 1;
    auto it = output.begin();
    do {
        std::string qhead = *it;
        ret = write(_socket, &qhead[0] + _head_offset, qhead.size() - _head_offset);
        if (ret > 0){
            _head_offset += ret;
            if (_head_offset >= it->size()){
                it++;
                _head_offset = 0;
            }
        }
    } while (ret > 0 && it != output.end());
    output.erase(output.begin(), it);

    if (-1 == ret && errno != EAGAIN){
        _is_alive = false;
        _pLogger->debug("Failed to write to socket {}", _socket);
    }

    if (output.size() < MAX_OUTPUT_QUEUE_SIZE){
        _event.events |= EPOLLIN;
    }
    if (output.empty()){
        if (_eof){
            _is_alive = false;
        }
        _event.events &= ~EPOLLOUT;
    }
}

} // namespace STnonblock
} // namespace Network
} // namespace Afina
