#ifndef AFINA_STORAGE_THREAD_SAFE_SIMPLE_LRU_H
#define AFINA_STORAGE_THREAD_SAFE_SIMPLE_LRU_H

#include <map>
#include <mutex>
#include <string>

#include "SimpleLRU.h"

namespace Afina {
namespace Backend {

/**
 * # SimpleLRU thread safe version
 *
 *
 */
class ThreadSafeSimplLRU : public SimpleLRU {
public:
    ThreadSafeSimplLRU(size_t max_size = 1024) : SimpleLRU(max_size) {}
    ~ThreadSafeSimplLRU() {}

    // see SimpleLRU.h
    bool Put(const std::string &key, const std::string &value) override {
        // TODO: sinchronization
        _m.lock();
        return SimpleLRU::Put(key, value);
        _m.unlock();
    }

    // see SimpleLRU.h
    bool PutIfAbsent(const std::string &key, const std::string &value) override {
        // TODO: sinchronization
        _m.lock();
        return SimpleLRU::PutIfAbsent(key, value);
        _m.unlock();
    }

    // see SimpleLRU.h
    bool Set(const std::string &key, const std::string &value) override {
        // TODO: sinchronization
        _m.lock();
        return SimpleLRU::Set(key, value);
        _m.unlock();
    }

    // see SimpleLRU.h
    bool Delete(const std::string &key) override {
        // TODO: sinchronization
        _m.lock();
        return SimpleLRU::Delete(key);
        _m.unlock();
    }

    // see SimpleLRU.h
    bool Get(const std::string &key, std::string &value) override {
        // TODO: sinchronization
        _m.lock();
        return SimpleLRU::Get(key, value);
        _m.unlock();
    }

private:
    std::mutex _m;
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_THREAD_SAFE_SIMPLE_LRU_H
