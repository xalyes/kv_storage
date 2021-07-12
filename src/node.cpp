#include <fstream>

#include "node.h"
#include "utils.h"

namespace kv_storage {

std::string Node::Get(Key key) const
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, ptrs[m_keyCount]);

    return foundChild->Get(key);
}

std::optional<CreatedBPNode> Node::Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot)
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, ptrs[m_keyCount]);

    auto newNode = foundChild->Put(key, value, nodesCount, false);
    if (!newNode)
    {
        return std::nullopt;
    }
    // Child splitted

    if (m_keyCount == MaxKeys)
    {
        // Must split this node
        uint32_t copyCount = MaxKeys / 2;

        std::array<Key, MaxKeys> newKeys;
        newKeys.fill(0);
        std::array<FileIndex, B> newPtrs;
        newPtrs.fill(0);

        uint32_t borderIndex = MaxKeys - copyCount;

        std::swap(m_keys[borderIndex], newKeys[0]);
        std::swap(ptrs[borderIndex + 1], newPtrs[0]);

        for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
        {
            std::swap(m_keys[i], newKeys[i - borderIndex]);
            std::swap(ptrs[i + 1], newPtrs[i - borderIndex]);
        }

        m_keyCount -= copyCount;

        const auto insert = [](uint32_t& count, std::array<Key, MaxKeys>& keys, std::array<FileIndex, B>& ptrs, Key newKey, FileIndex newIdx)
        {
            for (uint32_t i = 0; i < count; i++)
            {
                auto currentKey = keys[i];

                if (newKey >= currentKey)
                    continue;

                InsertToArray(keys, i, newKey);
                InsertToArray(ptrs, i, newIdx);
                count++;
                return;
            }

            InsertToArray(keys, count, newKey);
            InsertToArray(ptrs, count, newIdx);
            count++;
        };

        Key firstNewKey = newNode.value().key;
        if (key < newKeys[0])
        {
            bool found = false;
            // insert to this
            for (uint32_t i = 0; i < m_keyCount; i++)
            {
                auto currentKey = m_keys[i];

                if (firstNewKey >= currentKey)
                    continue;

                InsertToArray(m_keys, i, firstNewKey);
                InsertToArray(ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                found = true;
                break;
            }

            if (!found)
            {
                InsertToArray(m_keys, m_keyCount, firstNewKey);
                InsertToArray(ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
                m_keyCount++;
            }
        }
        else
        {
            // insert to new node
            insert(copyCount, newKeys, newPtrs, firstNewKey, newNode.value().node->GetIndex());
        }

        auto keyToDelete = newKeys[0];
        RemoveFromArray(newKeys, 0);
        copyCount--;

        auto newNode = std::make_unique<Node>(m_dir, ++nodesCount, copyCount, std::move(newKeys), std::move(newPtrs));

        if (isRoot)
            m_index = ++nodesCount;

        newNode->Flush();
        Flush();

        return std::optional<CreatedBPNode>({ std::move(newNode), keyToDelete });
    }
    else
    {
        Key keyForInsert = newNode.value().key;

        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            if (keyForInsert < m_keys[i])
            {
                InsertToArray(m_keys, i, keyForInsert);
                InsertToArray(ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                Flush();
                return std::nullopt;
            }
        }

        InsertToArray(m_keys, m_keyCount, keyForInsert);
        InsertToArray(ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
        m_keyCount++;

        Flush();
        return std::nullopt;
    }
}

void Node::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary);

    // TODO: check first char is 11
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    in.read(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

void Node::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("8", 1);

    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    out.write(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

} // kv_storage
