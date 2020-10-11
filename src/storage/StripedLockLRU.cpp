#include "StripedLockLRU.h"

namespace Afina {
namespace Backend {

StripedLockLRU::StripedLockLRU(std::size_t max_size, std::size_t n_shards) : _n_shards(n_shards) {
    std::size_t shard_limit = max_size / n_shards;
    if (shard_limit < 1024 * 1024) {
        throw std::runtime_error("STORAGE CREATION ERROR: too many shards");
    }
    for (std::size_t i = 0; i < n_shards; ++i){
        shards.emplace_back(new ThreadSafeSimplLRU(shard_limit));
    }
}

// See MapBasedGlobalLockImpl.h
bool StripedLockLRU::Put(const std::string &key, const std::string &value) { 
    return shards[hash(key) % _n_shards]->Put(key, value);
}

// See MapBasedGlobalLockImpl.h
bool StripedLockLRU::PutIfAbsent(const std::string &key, const std::string &value) { 
    return shards[hash(key) % _n_shards]->PutIfAbsent(key, value);
}

// See MapBasedGlobalLockImpl.h
bool StripedLockLRU::Set(const std::string &key, const std::string &value) { 
    return shards[hash(key) % _n_shards]->Set(key, value);
}

// See MapBasedGlobalLockImpl.h
bool StripedLockLRU::Delete(const std::string &key) {
    return shards[hash(key) % _n_shards]->Delete(key);
}

// See MapBasedGlobalLockImpl.h
bool StripedLockLRU::Get(const std::string &key, std::string &value) { 
    return shards[hash(key) % _n_shards]->Get(key, value);
}

} // namespace Backend
} // namespace Afina
