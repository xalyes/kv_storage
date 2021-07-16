#ifndef UTILS_H
#define UTILS_H

#include <array>

namespace kv_storage {

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
        if (!fs::exists(dir / ("batch_" + std::to_string(++index) + ".dat")))
            return index;
    }
}

inline void Remove(const fs::path& dir, FileIndex index)
{
    fs::remove(dir / ("batch_" + std::to_string(index) + ".dat"));
}

} // kv_storage

#endif // UTILS_H
