#ifndef AFINA_STORAGE_SIMPLE_LRU_H
#define AFINA_STORAGE_SIMPLE_LRU_H

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <afina/Storage.h>

namespace Afina {
namespace Backend {

/**
 * # Map based implementation
 * That is NOT thread safe implementaiton!!
 */
class SimpleLRU : public Afina::Storage {
public:
    SimpleLRU(size_t max_size = 1024) :
        _max_size(max_size),
        _cur_available(max_size),
        _lru_head(nullptr),
        _lru_tail(nullptr)
        {}

    ~SimpleLRU() {
        _lru_index.clear();
        if (_lru_tail){
            _lru_tail = _lru_tail->prev;
            while (_lru_tail){
                _lru_tail->next.reset();
                _lru_tail = _lru_tail->prev;
            }
            _lru_head.reset();
        }
    }

    // Implements Afina::Storage interface
    bool Put(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool PutIfAbsent(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Set(const std::string &key, const std::string &value) override;

    // Implements Afina::Storage interface
    bool Delete(const std::string &key) override;

    // Implements Afina::Storage interface
    bool Get(const std::string &key, std::string &value) override;

private:
    // LRU cache node
    using lru_node = struct lru_node {
        const std::string key;
        std::string value;
        lru_node* prev;
        std::unique_ptr<lru_node> next;
    };

    // Maximum number of bytes could be stored in this cache.
    // i.e all (keys+values) must be less than _max_size
    std::size_t _max_size;

    // Maximum number of bytes could be stored currently (with no deletions).
    // must be less than _max_size
    std::size_t _cur_available;

    // Main storage of lru_nodes, elements in this list ordered descending by "freshness": in the head
    // element that wasn't used for longest time.
    //
    // List owns all nodes
    std::unique_ptr<lru_node> _lru_head;
    lru_node* _lru_tail;

    // Index of nodes from list above, allows fast random access to elements by lru_node#key
    std::map<std::reference_wrapper<const std::string>, std::reference_wrapper<lru_node>, std::less<std::string>> _lru_index;

    // Moves node to tail of the list, so that node becomes "the freshest".
    void moveToTail(lru_node& node);

    // Adds a new node to the tail of the list.
    void addNode(const std::string& key, const std::string& value);

    // Deletes an element that wasn;t used for the longest time.
    void deleteOneFromHead();

    // Changes value of existing node. This node moves to the tail of the list.
    void changeValue(lru_node& node, const std::string& value);
};

} // namespace Backend
} // namespace Afina

#endif // AFINA_STORAGE_SIMPLE_LRU_H
