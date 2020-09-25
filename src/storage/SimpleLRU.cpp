#include "SimpleLRU.h"

namespace Afina {
namespace Backend {

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Put(const std::string &key, const std::string &value) { 
    std::size_t put_size = key.size() + value.size();
    if (put_size > _max_size){
        return false; 
    }
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()){
        while (put_size > _cur_available){
            deleteOneFromHead();
        }
        addNode(key, value);
        return true;
    } else {
        changeValue(it->second.get(), value);
        return true;
    }
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::PutIfAbsent(const std::string &key, const std::string &value) { 
    std::size_t put_size = key.size() + value.size();
    if (put_size > _max_size){
        return false;
    }
    if (_lru_index.find(key) != _lru_index.end()){
        return false;
    }
    while (put_size > _cur_available){
        deleteOneFromHead();
    }
    addNode(key, value);
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Set(const std::string &key, const std::string &value) { 
    if (key.size() + value.size() > _max_size){
        return false;
    }
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()){
        return false;
    }
    changeValue(it->second.get(), value);
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Delete(const std::string &key) {
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()){
        return false;
    }
    lru_node& node = it->second.get();
    if (node.prev == nullptr){
        deleteOneFromHead();
    } else {
        _lru_index.erase(it);
        _cur_available += key.size() + node.value.size();
        if (node.next == nullptr){
            _lru_tail = node.prev;
            _lru_tail->next.reset();
        } else {
            node.next->prev = node.prev;
            node.prev->next = std::move(node.next);
        }
    }
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Get(const std::string &key, std::string &value) { 
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()){
        return false;
    }
    value = it->second.get().value;
    moveToTail(it->second.get());
    return true;
}

void SimpleLRU::addNode(const std::string& key, const std::string& value){
    lru_node* node = new lru_node {key, value, nullptr, nullptr};
    if (_lru_head != nullptr){
        node->prev = _lru_tail;
        _lru_tail->next.reset(node);
        _lru_tail = node;
    } else {
        node->prev = nullptr;
        _lru_tail = node;
        _lru_head.reset(node);
    }
    _cur_available -= key.size() + value.size();
    _lru_index.insert(std::make_pair(std::reference_wrapper<const std::string>(_lru_tail->key), std::reference_wrapper<lru_node>(*_lru_tail)));
}

void SimpleLRU::changeValue(lru_node& node, const std::string& value){
    moveToTail(node);
    std::size_t diff_in_size  = 0;
    if (node.value.size() > value.size()){
        diff_in_size = node.value.size() - value.size();
        _cur_available += diff_in_size;
    } else {
        diff_in_size = value.size() - node.value.size();
        while (diff_in_size > _cur_available){
            deleteOneFromHead();
        }
        _cur_available -= diff_in_size;
    }
    node.value = value;
}

void SimpleLRU::moveToTail(lru_node& node){
    if (node.next == nullptr){
        return;
    }
    if (node.prev == nullptr){
        node.next->prev = nullptr;
        _lru_tail->next = std::move(_lru_head);
        _lru_head = std::move(node.next);
    } else {
        _lru_tail->next = std::move(node.prev->next);
        node.next->prev = node.prev;
        node.prev->next = std::move(node.next);
    }
    node.prev = _lru_tail;
    node.next = nullptr;
    _lru_tail = &node;
}

void SimpleLRU::deleteOneFromHead(){
    _cur_available += _lru_head->key.size() + _lru_head->value.size();
    _lru_index.erase(_lru_head->key);
    if (_lru_head->next == nullptr){
        _lru_tail = nullptr;
        _lru_head.reset();
    } else {
        _lru_head = std::move(_lru_head->next);
        _lru_head->prev = nullptr;
    }
}

} // namespace Backend
} // namespace Afina
