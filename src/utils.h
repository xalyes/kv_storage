#ifndef UTILS_H
#define UTILS_H

#include <map>
#include <list>
#include <array>
#include <optional>

namespace fs = std::filesystem;

namespace kv_storage {

using FileIndex = uint64_t;

template<uint32_t N>
void InsertToSortedArray(std::array<uint64_t, N>& arr, uint32_t count, uint64_t value)
{
    // TODO: binary search may be?
    for (uint32_t i = 0; i < count; i++)
    {
        if (value < arr[i])
        {
            InsertToArray(arr, i, value);
            return;
        }
    }

    InsertToArray(arr, count, value);
}

template<size_t N>
void InsertToArray(std::array<uint64_t, N>& arr, size_t pos, uint64_t val)
{
    if (pos > arr.size())
        throw std::runtime_error("Invalid position for insert to std array");

    if (pos != arr.max_size())
    {
        for (size_t i = arr.size() - 1; i > pos; i--)
        {
            std::swap(arr[i], arr[i - 1]);
        }
    }
    arr[pos] = val;
}

template<size_t N>
void RemoveFromArray(std::array<uint64_t, N>& arr, size_t pos)
{
    if (pos > arr.size())
        throw std::runtime_error("Invalid position for removing from std array");

    arr[pos] = 0;
    if (pos != arr.max_size() - 1)
    {
        for (size_t i = pos; i < arr.max_size() - 1; i++)
        {
            std::swap(arr[i], arr[i + 1]);
        }
    }
}

inline FileIndex FindFreeIndex(const fs::path& dir, FileIndex begin)
{
    auto index = begin;
    while (true)
    {
        if (!fs::exists(dir / ("batch_" + std::to_string(++index) + ".dat")) && index != 0 && index != 1)
            return index;
    }
}

inline void Remove(const fs::path& dir, FileIndex index)
{
    fs::remove(dir / ("batch_" + std::to_string(index) + ".dat"));
}

// a cache which evicts the least recently used item when it is full
// modified boost cache from boost/compute/detail/lru_cache.hpp
template<class Key, class Value>
class lru_cache
{
public:
    typedef Key key_type;
    typedef Value value_type;
    typedef std::list<key_type> list_type;
    typedef std::map<
                key_type,
                std::pair<value_type, typename list_type::iterator>
            > map_type;

    lru_cache(size_t capacity)
        : m_capacity(capacity)
    {
    }

    ~lru_cache()
    {
    }

    size_t size() const
    {
        return m_map.size();
    }

    size_t capacity() const
    {
        return m_capacity;
    }

    bool empty() const
    {
        return m_map.empty();
    }

    bool contains(const key_type &key)
    {
        return m_map.find(key) != m_map.end();
    }

    bool erase(const key_type& key)
    {
        typename map_type::iterator i = m_map.find(key);
        if (i != m_map.end())
        {
            m_list.erase(i->second.second);
            m_map.erase(i);
            return true;
        }
        return false;
    }

    void insert(const key_type &key, const value_type &value)
    {
        typename map_type::iterator i = m_map.find(key);
        if (i != m_map.end())
        {
            m_list.erase(i->second.second);
            m_map.erase(i);
            i = m_map.end();
        }

        if(i == m_map.end()){
            // insert item into the cache, but first check if it is full
            if(size() >= m_capacity){
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            m_list.push_front(key);
            m_map[key] = std::make_pair(value, m_list.begin());
        }
    }

    std::optional<value_type> get(const key_type &key)
    {
        // lookup value in the cache
        typename map_type::iterator i = m_map.find(key);
        if(i == m_map.end()){
            // value not in cache
            return std::nullopt;
        }

        // return the value, but first update its place in the most
        // recently used list
        typename list_type::iterator j = i->second.second;
        if(j != m_list.begin()){
            // move item to the front of the most recently used list
            m_list.erase(j);
            m_list.push_front(key);

            // update iterator in map
            j = m_list.begin();
            const value_type &value = i->second.first;
            m_map[key] = std::make_pair(value, j);

            // return the value
            return value;
        }
        else {
            // the item is already at the front of the most recently
            // used list so just return it
            return i->second.first;
        }
    }

    void clear()
    {
        m_map.clear();
        m_list.clear();
    }

private:
    void evict()
    {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --m_list.end();
        m_map.erase(*i);
        m_list.erase(i);
    }

private:
    map_type m_map;
    list_type m_list;
    size_t m_capacity;
};

} // kv_storage

#endif // UTILS_H
