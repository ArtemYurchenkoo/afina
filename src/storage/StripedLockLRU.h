#ifndef AFINA_STORAGE_STRIPED_LOCK_LRU_H
#define AFINA_STORAGE_STRIPED_LOCK_LRU_H

#include <map>
#include <mutex>
#include <string>
#include <functional>
#include <vector>

#include "ThreadSafeSimpleLRU.h"

namespace Afina {
namespace Backend {

/**
 * # SimpleLRU thread safe version
 */
class StripedLockLRU : public ThreadSafeSimplLRU {
public:
    StripedLockLRU(std::size_t max_size = 1024 * 1024 * 8, std::size_t n_shards = 4);
    ~StripedLockLRU() {}

    // see SimpleLRU.h
    bool Put(const std::string &key, const std::string &value) override;

    // see SimpleLRU.h
    bool PutIfAbsent(const std::string &key, const std::string &value) override;

    // see SimpleLRU.h
    bool Set(const std::string &key, const std::string &value) override;

    // see SimpleLRU.h
    bool Delete(const std::string &key) override;

    // see SimpleLRU.h
    bool Get(const std::string &key, std::string &value) override;

private:
    std::hash<std::string> hash;
    std::vector<std::unique_ptr<ThreadSafeSimplLRU>> shards;
    std::size_t _n_shards;
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_STRIPED_LOCK_LRU_H