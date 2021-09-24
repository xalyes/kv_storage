#ifndef UTILS_H
#define UTILS_H

#include <map>
#include <list>
#include <array>
#include <optional>
#include <atomic>
#include <functional>
#include <filesystem>
#include <boost/endian/conversion.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/locks.hpp>

namespace fs = std::filesystem;

#undef max

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
        for (int i = 0; i < size / 2; i++)
        {
            std::swap(pVal[size - 1 - i], pVal[i]);
        }
    }
}

//-------------------------------------------------------------------------------
//                              lfu_cache
//-------------------------------------------------------------------------------
// a cache which evicts the least frequently used item when it is full
// modified boost cache from boost/compute/detail/lru_cache.hpp
template<class Key, class Value>
class lfu_cache
{
public:
    typedef Key key_type;
    typedef Value value_type;
    typedef std::list<key_type> list_type;
    typedef std::map<
                key_type,
                std::pair<value_type, std::atomic_uint32_t>
            > map_type;

    lfu_cache(size_t capacity, std::function<void(Value&)> disposer = [](Value& v) {})
        : m_capacity(capacity)
        , m_disposer(disposer)
    {
    }

    ~lfu_cache()
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
            m_map.erase(i);
            i = m_map.end();
        }

        // insert item into the cache, but first check if it is full
        if(m_map.size() >= m_capacity)
        {
            // cache is full, evict the least frequently used item
            evict();
        }

        // insert the new item
        m_map[key] = std::make_pair(value, 0U);
    }

    std::optional<value_type> get(const key_type &key)
    {
        boost::shared_lock<boost::shared_mutex> lock(m_mutex);
        // lookup value in the cache
        typename map_type::iterator i = m_map.find(key);
        if(i == m_map.end()){
            // value not in cache
            return std::nullopt;
        }

        if (i->second.second == std::numeric_limits<uint32_t>::max() - 1)
        {
            lock.unlock();
            boost::unique_lock<boost::shared_mutex> uniqueLock(m_mutex);

            typename map_type::iterator newIt = m_map.find(key);
            if (newIt == m_map.end()) {
                // value not in cache
                return std::nullopt;
            }

            typename map_type::iterator minIt;
            for (auto it = m_map.begin(); it != m_map.end(); it++)
            {
                if (minIt->second.second > it->second.second)
                    minIt = it;
            }

            ++newIt->second.second;

            if (minIt->second.second == 0)
                return newIt->second.first;

            uint32_t subValue = minIt->second.second;
            for (auto it = m_map.begin(); it != m_map.end(); it++)
            {
                if (it->second.second >= subValue)
                    it->second.second -= subValue;
                else
                    it->second.second = 0;
            }
            return newIt->second.first;
        }
        else
        {
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
    }

private:
    void evict()
    {
        typename map_type::iterator minIt;
        for (auto it = m_map.begin(); it != m_map.end(); it++)
        {
            if (minIt->second.second > it->second.second)
                minIt = it;
        }

        m_disposer(minIt->second.first);
        m_map.erase(minIt);
    }

private:
    map_type m_map;
    size_t m_capacity;
    mutable boost::shared_mutex m_mutex;
    std::function<void(Value&)> m_disposer;
};

} // kv_storage

#endif // UTILS_H
