#ifndef UTILS_H
#define UTILS_H

#include <array>

namespace kv_storage {

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

template<class T, size_t N>
void RemoveFromArray(std::array<T, N>& arr, size_t pos)
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

} // kv_storage

#endif // UTILS_H
