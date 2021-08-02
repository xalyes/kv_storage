#ifndef UTILS_H
#define UTILS_H

#include <map>
#include <list>
#include <array>
#include <optional>
#include <functional>
#include <filesystem>
#include <boost/endian/conversion.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace fs = std::filesystem;

namespace kv_storage {

using FileIndex = uint64_t;

//-------------------------------------------------------------------------------
constexpr uint32_t Half(uint32_t num)
{
    if (num % 2 == 0)
        return (num / 2) - 1;
    else
        return (num - 1) / 2;
}

//-------------------------------------------------------------------------------
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

//-------------------------------------------------------------------------------
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

//-------------------------------------------------------------------------------
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

//-------------------------------------------------------------------------------
//                              IndexManager
//-------------------------------------------------------------------------------
class IndexManager
{
public:
    IndexManager(const fs::path& dir)
        : m_dir(dir)
    {}

    FileIndex FindFreeIndex(const fs::path& dir)
    {
        boost::unique_lock<boost::mutex> lock(m_mutex);
        while (true)
        {
            if (!fs::exists(dir / ("batch_" + std::to_string(++m_currentIndex) + ".dat")) && m_currentIndex != 0 && m_currentIndex != 1)
                return m_currentIndex;
        }
    }

    void Remove(const fs::path& dir, FileIndex index)
    {
        boost::unique_lock<boost::mutex> lock(m_mutex);
        fs::remove(dir / ("batch_" + std::to_string(index) + ".dat"));
    }

private:
    const fs::path m_dir;
    boost::mutex m_mutex;
    FileIndex m_currentIndex{ 1 };
};

//-------------------------------------------------------------------------------
template <typename T>
T NativeToLittleEndian(T val)
{
    if (boost::endian::order::native == boost::endian::order::big)
    {
        T res;
        char* pVal = (char*)&val;
        char* pRes = (char*)&res;
        int size = sizeof(T);
        for (int i = 0; i < size; i++)
        {
            pRes[size - 1 - i] = pVal[i];
        }

        return res;
    }
    else
    {
        return val;
    }
}

//-------------------------------------------------------------------------------
template <typename T>
void LittleToNativeEndianInplace(T& val)
{
    if (boost::endian::order::native == boost::endian::order::big)
    {
        char* pVal = (char*)&val;
        int size = sizeof(T);
        for (int i = 0; i < size; i++)
        {
            pVal[size - 1 - i] = pVal[i];
        }
    }
}

//-------------------------------------------------------------------------------
//                              lru_cache
//-------------------------------------------------------------------------------
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

    lru_cache(size_t capacity, std::function<void(Value&)> disposer = [](Value& v) {})
        : m_capacity(capacity)
        , m_disposer(disposer)
    {
    }

    ~lru_cache()
    {
        try
        {
            clear();
        }
        catch (...)
        {
        }
    }

    size_t size() const
    {
        boost::shared_lock<boost::shared_mutex> lock(m_mutex);
        return m_map.size();
    }

    size_t capacity() const
    {
        boost::shared_lock<boost::shared_mutex> lock(m_mutex);
        return m_capacity;
    }

    bool empty() const
    {
        boost::shared_lock<boost::shared_mutex> lock(m_mutex);
        return m_map.empty();
    }

    bool contains(const key_type &key)
    {
        boost::shared_lock<boost::shared_mutex> lock(m_mutex);
        return m_map.find(key) != m_map.end();
    }

    bool erase(const key_type& key)
    {
        boost::unique_lock<boost::shared_mutex> lock(m_mutex);
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
        boost::unique_lock<boost::shared_mutex> lock(m_mutex);
        typename map_type::iterator i = m_map.find(key);
        if (i != m_map.end())
        {
            m_list.erase(i->second.second);
            m_map.erase(i);
            i = m_map.end();
        }

        if(i == m_map.end()){
            // insert item into the cache, but first check if it is full
            if(m_map.size() >= m_capacity){
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
        boost::unique_lock<boost::shared_mutex> lock(m_mutex);
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
        boost::unique_lock<boost::shared_mutex> lock(m_mutex);

        for (auto& item : m_map)
        {
            m_disposer(item.second.first);
        }

        m_map.clear();
        m_list.clear();
    }

private:
    void evict()
    {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --m_list.end();
        auto it = m_map.find(*i);
        m_disposer(it->second.first);

        m_map.erase(it);
        m_list.erase(i);
    }

private:
    map_type m_map;
    list_type m_list;
    size_t m_capacity;
    mutable boost::shared_mutex m_mutex;
    std::function<void(Value&)> m_disposer;
};

} // kv_storage

#endif // UTILS_H
