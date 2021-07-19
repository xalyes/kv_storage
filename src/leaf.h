#ifndef LEAF_H
#define LEAF_H

#include <fstream>
#include <optional>

#include "bp_node.h"

namespace kv_storage {

using Offset = uint64_t;

template <class V>
class Leaf
    : public BPNode<V>
    , public std::enable_shared_from_this<BPNode<V>>
{
public:
    template<class V> friend class VolumeEnumerator;

    Leaf(const fs::path& dir, BPCache<V>& cache, FileIndex idx)
        : BPNode(dir, cache, idx)
    {
    }

    Leaf(const fs::path& dir, BPCache<V>& cache, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::vector<V>&& newValues, FileIndex newNextBatch)
        : BPNode<V>(dir, cache, idx, newKeyCount, std::move(newKeys))
        , m_values(newValues)
        , m_nextBatch(newNextBatch)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode<V>> Put(Key key, const V& val, FileIndex& nodesCount) override;
    virtual V Get(Key key) const override;
    virtual DeleteResult<V> Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling) override;
    virtual Key GetMinimum() const override;
    virtual std::shared_ptr<BPNode<V>> GetFirstLeaf() override;

private:
    CreatedBPNode<V> SplitAndPut(Key key, const V& value, FileIndex& nodesCount);
    void LeftJoin(const Leaf<V>& leaf);
    void RightJoin(const Leaf<V>& leaf);
    void Insert(Key key, const V& value, uint32_t pos);

private:
    std::vector<V> m_values;
    FileIndex m_nextBatch{ 0 };
};

template <class V>
std::shared_ptr<BPNode<V>> Leaf<V>::GetFirstLeaf()
{
    return shared_from_this();
}

template<class V>
void Leaf<V>::Insert(Key key, const V& value, uint32_t pos)
{
    InsertToArray(m_keys, pos, key);
    m_values.insert(m_values.begin() + pos, value);
    m_keyCount++;
}

template<class V>
std::optional<CreatedBPNode<V>> Leaf<V>::Put(Key key, const V& val, FileIndex& nodesCount)
{
    if (m_keyCount == MaxKeys)
    {
        if (m_index == 1)
        {
            nodesCount = FindFreeIndex(m_dir, nodesCount);
            m_index = nodesCount;
        }

        return SplitAndPut(key, val, nodesCount);
    }

    if (m_keyCount == 0)
    {
        Insert(key, val, 0);
    }
    else
    {
        for (size_t i = 0; i < m_keyCount; i++)
        {
            auto currentKey = m_keys[i];

            if (key < currentKey)
            {
                if (m_keyCount != MaxKeys)
                {
                    Insert(key, val, i);
                }
                else
                {
                    throw std::runtime_error("Try to insert value to fullfilled Leaf");
                }
                Flush();
                return std::nullopt;
            }

            if (currentKey == key)
            {
                throw std::runtime_error("Couldn't insert exising key");
            }
        }

        Insert(key, val, m_keyCount);
    }
    Flush();
    return std::nullopt;
}

template<class V>
CreatedBPNode<V> Leaf<V>::SplitAndPut(Key key, const V& value, FileIndex& nodesCount)
{
    uint32_t copyCount = MaxKeys / 2;

    std::array<Key, MaxKeys> newKeys;
    newKeys.fill(0);
    std::array<Offset, MaxKeys> newOffsets;
    newOffsets.fill(0);

    std::vector<std::string> newValues;

    uint32_t borderIndex = m_values.size() - copyCount;

    newValues.insert(newValues.end(), std::make_move_iterator(m_values.begin() + borderIndex),
        std::make_move_iterator(m_values.end()));
    m_values.erase(m_values.begin() + borderIndex, m_values.end());

    std::swap(m_keys[borderIndex], newKeys[0]);
    newOffsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(m_keys) + 1;

    for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
    {
        std::swap(m_keys[i], newKeys[i - borderIndex]);

        newOffsets[i - borderIndex] = newOffsets[i - borderIndex - 1] + newValues[i - borderIndex - 1].size();
    }

    m_keyCount -= copyCount;

    Key firstNewKey = newKeys[0];

    nodesCount = FindFreeIndex(m_dir, nodesCount);
    auto newLeaf = std::make_shared<Leaf>(m_dir, m_cache, nodesCount, copyCount, std::move(newKeys), std::move(newValues), m_nextBatch);
    m_cache.insert(nodesCount, newLeaf);

    m_nextBatch = newLeaf->m_index;

    if (key < firstNewKey)
    {
        Put(key, value, nodesCount);
        newLeaf->Flush();
    }
    else
    {
        Flush();
        newLeaf->Put(key, value, nodesCount);
    }

    return { std::move(newLeaf), firstNewKey };
}

template<class V>
V Leaf<V>::Get(Key key) const
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            return m_values[i];
        }
    }

    throw std::runtime_error("Failed to get unexisted value of key '" + std::to_string(key) + "'");
}

template<class V>
void Leaf<V>::LeftJoin(const Leaf<V>& leaf)
{
    std::array<Key, MaxKeys> newKeys = leaf.m_keys;

    for (uint32_t i = leaf.m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        newKeys[i] = m_keys[i - leaf.m_keyCount];
    }
    m_keys = std::move(newKeys);
    m_values.insert(m_values.begin(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
    m_index = leaf.m_index;
}

template<class V>
void Leaf<V>::RightJoin(const Leaf<V>& leaf)
{
    for (uint32_t i = m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        m_keys[i] = leaf.m_keys[i - m_keyCount];
    }

    m_values.insert(m_values.end(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
    m_nextBatch = leaf.m_nextBatch;
}

template<class V>
std::shared_ptr<BPNode<V>> CreateBPNode(const fs::path& dir, BPCache<V>& cache, FileIndex idx);

template<class V>
DeleteResult<V> Leaf<V>::Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling)
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            // 1. First of all remove key and value.
            RemoveFromArray(m_keys, i);
            m_values.erase(m_values.begin() + i);
            m_keyCount--;

            // 2. Check key count.
            // If we have too few keys and this leaf is not root we should make some additional changes.
            if (m_index == 1 || m_keyCount >= MinKeys)
            {
                Flush();
                return { DeleteType::Deleted, std::nullopt };
            }

            std::shared_ptr<Leaf> leftSiblingLeaf;
            std::shared_ptr<Leaf> rightSiblingLeaf;

            // 3. If left sibling has enough keys we can simple borrow the entry.
            if (leftSibling)
            {
                leftSiblingLeaf = std::static_pointer_cast<Leaf<V>>(CreateBPNode<V>(m_dir, m_cache, leftSibling->index));

                if (leftSiblingLeaf->m_keyCount > MinKeys)
                {
                    auto key = leftSiblingLeaf->GetLastKey();
                    auto value = leftSiblingLeaf->m_values[leftSiblingLeaf->m_keyCount - 1];
                    Insert(key, value, 0);
                    leftSiblingLeaf->Delete(key, std::nullopt, std::nullopt);
                    Flush();
                    return { DeleteType::BorrowedLeft, m_keys[0] };
                }
            }

            // 4. If right sibling has enough keys we can simple borrow the entry.
            if (rightSibling)
            {
                rightSiblingLeaf = std::static_pointer_cast<Leaf>(CreateBPNode<V>(m_dir, m_cache, rightSibling->index));

                if (rightSiblingLeaf->m_keyCount > MinKeys)
                {
                    auto key = rightSiblingLeaf->m_keys[0];
                    auto value = rightSiblingLeaf->m_values[0];
                    Insert(key, value, m_keyCount);
                    rightSiblingLeaf->Delete(key, std::nullopt, std::nullopt);
                    Flush();
                    return { DeleteType::BorrowedRight, rightSiblingLeaf->m_keys[0] };
                }
            }

            // 5. Both siblngs have too few keys. We should merge this leaf and sibling.
            if (leftSibling)
            {
                const auto currentIndex = m_index;
                LeftJoin(*leftSiblingLeaf);
                Flush();
                Remove(m_dir, currentIndex);
                m_cache.erase(currentIndex);

                return { DeleteType::MergedLeft, m_keys[0] };
            }
            else if (rightSibling)
            {
                RightJoin(*rightSiblingLeaf);
                Flush();
                Remove(m_dir, rightSiblingLeaf->GetIndex());
                m_cache.erase(rightSiblingLeaf->GetIndex());

                return { DeleteType::MergedRight, m_keys[0] };
            }
            else
            {
                throw std::runtime_error("Bad leaf status");
            }
        }
    }

    throw std::runtime_error("Failed to remove unexisted value of key '" + std::to_string(key) + "'");
}

template <class V>
Key Leaf<V>::GetMinimum() const
{
    return m_keys[0];
}

} // kv_storage

#endif // LEAF_H
