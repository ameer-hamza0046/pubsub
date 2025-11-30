#pragma once

#include <list>
#include <string>
#include <unordered_map>
#include <utility>

template <typename Key, typename Value>
class LRUCache {
   public:
    explicit LRUCache(size_t capacity) : capacity(capacity) {}

    // Returns true if found, and populates valueOut
    // Also moves the item to the front (Most Recently Used)
    bool get(const Key& key, Value& valueOut) {
        auto it = cache_map.find(key);
        if (it == cache_map.end()) {
            return false;
        }

        // Move this node to the front of the list
        lru_list.splice(lru_list.begin(), lru_list, it->second);
        valueOut = it->second->second;
        return true;
    }

    void put(const Key& key, const Value& value) {
        auto it = cache_map.find(key);
        if (it != cache_map.end()) {
            // Update existing value
            it->second->second = value;
            // Move to front
            lru_list.splice(lru_list.begin(), lru_list, it->second);
        } else {
            // Insert new
            // Check capacity
            if (cache_map.size() >= capacity) {
                // Evict LRU (back of list)
                auto last = lru_list.back();
                cache_map.erase(last.first);
                lru_list.pop_back();
            }

            // Push front
            lru_list.push_front({key, value});
            cache_map[key] = lru_list.begin();
        }
    }

   private:
    size_t capacity;
    // Doubly linked list of <Key, Value>
    std::list<std::pair<Key, Value>> lru_list;
    // Map of Key -> List Iterator
    std::unordered_map<Key, typename std::list<std::pair<Key, Value>>::iterator>
        cache_map;
};