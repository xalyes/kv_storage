#include <fstream>
#include <array>

#include "leaf.h"
#include "utils.h"

namespace kv_storage {

void Leaf::Insert(Key key, const std::string& value, uint32_t pos)
{
    InsertToArray(m_keys, pos, key);
    m_values.insert(m_values.begin() + pos, value);
    m_keyCount++;
}

std::optional<CreatedBPNode> Leaf::Put(Key key, const std::string& val, FileIndex& nodesCount)
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

CreatedBPNode Leaf::SplitAndPut(Key key, const std::string& value, FileIndex& nodesCount)
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

std::string Leaf::Get(Key key) const
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

void Leaf::LeftJoin(const Leaf& leaf)
{
    std::array<Key, MaxKeys> newKeys = leaf.m_keys;

    for (uint32_t i = leaf.m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        newKeys[i] = m_keys[i - leaf.m_keyCount];
    }
    m_keys = std::move(newKeys);
    m_values.insert(m_values.begin(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
}

void Leaf::RightJoin(const Leaf& leaf)
{
    for (uint32_t i = m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        m_keys[i] = leaf.m_keys[i - m_keyCount];
    }

    m_values.insert(m_values.end(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
    m_nextBatch = leaf.m_nextBatch;
}

DeleteResult Leaf::Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling)
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
                return { DeleteResult::Type::Deleted, std::nullopt };
            }

            std::shared_ptr<Leaf> leftSiblingLeaf;
            std::shared_ptr<Leaf> rightSiblingLeaf;

            // 3. If left sibling has enough keys we can simple borrow the entry.
            if (leftSibling)
            {
                leftSiblingLeaf = std::make_shared<Leaf>(m_dir, m_cache, leftSibling->index);
                leftSiblingLeaf->Load();
                m_cache.insert(leftSibling->index, leftSiblingLeaf);

                if (leftSiblingLeaf->m_keyCount > MinKeys)
                {
                    auto key = leftSiblingLeaf->GetLastKey();
                    auto value = leftSiblingLeaf->m_values[leftSiblingLeaf->m_keyCount - 1];
                    Insert(key, value, 0);
                    leftSiblingLeaf->Delete(key, std::nullopt, std::nullopt);
                    Flush();
                    return { DeleteResult::Type::BorrowedLeft, m_keys[0] };
                }
            }

            // 4. If right sibling has enough keys we can simple borrow the entry.
            if (rightSibling)
            {
                rightSiblingLeaf = std::make_shared<Leaf>(m_dir, m_cache, rightSibling->index);
                rightSiblingLeaf->Load();
                m_cache.insert(rightSibling->index, rightSiblingLeaf);

                if (rightSiblingLeaf->m_keyCount > MinKeys)
                {
                    auto key = rightSiblingLeaf->m_keys[0];
                    auto value = rightSiblingLeaf->m_values[0];
                    Insert(key, value, m_keyCount);
                    rightSiblingLeaf->Delete(key, std::nullopt, std::nullopt);
                    Flush();
                    return { DeleteResult::Type::BorrowedRight, rightSiblingLeaf->m_keys[0] };
                }
            }

            // 5. Both siblngs have too few keys. We should merge this leaf and sibling.
            if (leftSibling)
            {
                LeftJoin(*leftSiblingLeaf);
                Flush();
                Remove(m_dir, leftSiblingLeaf->GetIndex());

                return { DeleteResult::Type::MergedLeft, m_keys[0] };
            }
            else if (rightSibling)
            {
                RightJoin(*rightSiblingLeaf);
                Flush();
                Remove(m_dir, rightSiblingLeaf->GetIndex());

                return { DeleteResult::Type::MergedRight, m_keys[0] };
            }
            else
            {
                throw std::runtime_error("Bad leaf status");
            }
        }
    }

    throw std::runtime_error("Failed to remove unexisted value of key '" + std::to_string(key) + "'");
}

Key Leaf::GetMinimum() const
{
    return m_keys[0];
}

void Leaf::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();

    // TODO: check first char is "9"
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));

    std::array<Offset, MaxKeys> offsets;
    in.read(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));

    for (uint64_t i = 0; i < m_keyCount; i++)
    {
        auto offset = offsets[i];
        uint64_t endOffset;

        if (i == m_keyCount - 1)
            endOffset = filesize - sizeof(m_nextBatch);
        else
            endOffset = offsets[i + 1];

        auto size = endOffset - offset;

        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        m_values.emplace_back(buf.get());
    }

    in.read(reinterpret_cast<char*>(&(m_nextBatch)), sizeof(m_nextBatch));
}

void Leaf::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);
    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));

    std::array<Offset, MaxKeys> offsets;
    offsets.fill(0);

    auto offsetPos = out.tellp();
    offsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(offsets) + 1;
    out.write(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));

    uint32_t i = 0;
    for (uint32_t i = 0; i < m_values.size(); i++)
    {
        out.write(m_values[i].data(), m_values[i].size());
        if (i && i < MaxKeys)
            offsets[i] = offsets[i - 1] + m_values[i - 1].size();
    }

    out.write(reinterpret_cast<char*>(&(m_nextBatch)), sizeof(m_nextBatch));

    out.seekp(offsetPos);
    out.write(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));
}

} // kv_storage
