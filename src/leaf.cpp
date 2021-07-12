#include <fstream>

#include "leaf.h"
#include "utils.h"

namespace kv_storage {

std::optional<CreatedBPNode> Leaf::Put(Key key, const std::string& val, FileIndex& nodesCount, bool isRoot)
{
    if (m_keyCount == MaxKeys)
    {
        if (isRoot)
            m_index = ++nodesCount;

        return SplitAndPut(key, val, nextBatch, nodesCount);
    }

    if (m_keyCount == 0)
    {
        InsertToArray(m_keys, 0, key);

        values.push_back(val);
        InsertToArray(valuesOffsets, 0, sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1);

        m_keyCount++;
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
                    InsertToArray(m_keys, i, key);

                    values.insert(values.begin() + i, val);
                    if (i != 0)
                        InsertToArray(valuesOffsets, i, valuesOffsets[i - 1] + values[i - 1].size());
                    else
                        InsertToArray(valuesOffsets, 0, sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1);

                    m_keyCount++;

                    for (size_t j = i + 1; j < m_keyCount; j++)
                    {
                        valuesOffsets[j] += val.size();
                    }
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

        size_t i = m_keyCount;
        m_keys[i] = key;

        values.push_back(val);
        valuesOffsets[i] = valuesOffsets[i - 1] + values[i - 1].size();

        m_keyCount++;
    }
    Flush();
    return std::nullopt;
}

CreatedBPNode Leaf::SplitAndPut(Key key, const std::string& value, FileIndex nextBatch, FileIndex& nodesCount)
{
    uint32_t copyCount = MaxKeys / 2;

    std::array<Key, MaxKeys> newKeys;
    newKeys.fill(0);
    std::array<Offset, MaxKeys> newOffsets;
    newOffsets.fill(0);

    std::vector<std::string> newValues;

    uint32_t borderIndex = values.size() - copyCount;

    newValues.insert(newValues.end(), std::make_move_iterator(values.begin() + borderIndex),
        std::make_move_iterator(values.end()));
    values.erase(values.begin() + borderIndex, values.end());

    std::swap(m_keys[borderIndex], newKeys[0]);
    newOffsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1;

    for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
    {
        std::swap(m_keys[i], newKeys[i - borderIndex]);

        newOffsets[i - borderIndex] = newOffsets[i - borderIndex - 1] + newValues[i - borderIndex - 1].size();
    }

    m_keyCount -= copyCount;

    Key firstNewKey = newKeys[0];

    auto newLeaf = std::make_unique<Leaf>(m_dir, ++nodesCount, copyCount, std::move(newKeys), std::move(newOffsets), std::move(newValues), nextBatch);

    if (key < firstNewKey)
    {
        Put(key, value, nodesCount, false);
        newLeaf->Flush();
    }
    else
    {
        Flush();
        newLeaf->Put(key, value, nodesCount, false);
    }

    return { std::move(newLeaf), firstNewKey };
}

std::string Leaf::Get(Key key) const
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            return values[i];
        }
    }

    throw std::runtime_error("Failed to get unexisted value of key '" + std::to_string(key) + "'");
}

void Leaf::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();

    // TODO: check first char is 22
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    in.read(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (uint64_t i = 0; i < m_keyCount; i++)
    {
        auto offset = valuesOffsets[i];
        uint64_t endOffset;

        if (i == m_keyCount - 1)
            endOffset = filesize - sizeof(nextBatch);
        else
            endOffset = valuesOffsets[i + 1];

        auto size = endOffset - offset;

        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        values.emplace_back(buf.get());
        //std::cout << "Reading " << buf << std::endl;
    }

    in.read(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

void Leaf::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);
    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    out.write(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (const auto& v : values)
    {
        out.write(v.data(), v.size());
        //std::cout << "Writing " << v << std::endl;
    }

    out.write(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

} // kv_storage
