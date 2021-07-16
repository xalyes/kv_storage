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
            foundChild = CreateBPNode(m_dir, m_ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, m_ptrs[m_keyCount]);

    return foundChild->Get(key);
}


std::optional<CreatedBPNode> Node::Put(Key key, const std::string& value, FileIndex& nodesCount)
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, m_ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, m_ptrs[m_keyCount]);

    auto newNode = foundChild->Put(key, value, nodesCount);
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
        std::swap(m_ptrs[borderIndex + 1], newPtrs[0]);

        for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
        {
            std::swap(m_keys[i], newKeys[i - borderIndex]);
            std::swap(m_ptrs[i + 1], newPtrs[i - borderIndex]);
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
                InsertToArray(m_ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                found = true;
                break;
            }

            if (!found)
            {
                InsertToArray(m_keys, m_keyCount, firstNewKey);
                InsertToArray(m_ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
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

        nodesCount = FindFreeIndex(m_dir, nodesCount);
        auto newNode = std::make_unique<Node>(m_dir, nodesCount, copyCount, std::move(newKeys), std::move(newPtrs));

        if (m_index == 1)
        {
            nodesCount = FindFreeIndex(m_dir, nodesCount);
            m_index = nodesCount;
        }

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
                InsertToArray(m_ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                Flush();
                return std::nullopt;
            }
        }

        InsertToArray(m_keys, m_keyCount, keyForInsert);
        InsertToArray(m_ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
        m_keyCount++;

        Flush();
        return std::nullopt;
    }
}


DeleteResult Node::Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling)
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, m_ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, m_ptrs[childPos]);

    std::optional<Sibling> childRightSibling;
    std::optional<Sibling> childLeftSibling;

    if (childPos == 0)
    {
        childRightSibling = { m_keys[childPos], m_ptrs[childPos + 1] };
    }
    else if (childPos == m_keyCount)
    {
        childLeftSibling = { m_keys[childPos - 1], m_ptrs[childPos - 1] };
    }
    else
    {
        childLeftSibling = { m_keys[childPos - 1], m_ptrs[childPos - 1] };
        childRightSibling = { m_keys[childPos], m_ptrs[childPos + 1] };
    }

    DeleteResult result = foundChild->Delete(key, childLeftSibling, childRightSibling);

    if (result.type == DeleteResult::Type::Deleted)
    {
        if (childPos && key == m_keys[childPos - 1])
        {
            m_keys[childPos - 1] = foundChild->GetMinimum();
            Flush();
        }
        return result;
    }

    if (result.type == DeleteResult::Type::BorrowedLeft)
    {
        m_keys[childPos - 1] = *result.key;
        Flush();
        return { DeleteResult::Type::Deleted, std::nullopt, nullptr };
    }

    if (result.type == DeleteResult::Type::BorrowedRight)
    {
        m_keys[childPos] = *result.key;
        Flush();
        return { DeleteResult::Type::Deleted, std::nullopt, nullptr };
    }

    if (result.type == DeleteResult::Type::MergedRight)
    {
        if (childPos > 1)
        {
            m_keys[childPos - 1] = *result.key;
        }

        RemoveFromArray(m_keys, childPos);
        RemoveFromArray(m_ptrs, childPos + 1);
    }
    else
    {
        if (childPos > 1)
        {
            m_keys[childPos - 1] = *result.key;
            RemoveFromArray(m_keys, childPos - 2);
            RemoveFromArray(m_ptrs, childPos - 1);
        }
        else if (childPos == 1)
        {
            RemoveFromArray(m_keys, 0);
            RemoveFromArray(m_ptrs, 0);
        }
    }
    m_keyCount--;

    if (m_index == 1 && m_keyCount == 0)
    {
        Remove(m_dir, foundChild->GetIndex());
        foundChild->SetIndex(1);
        foundChild->Flush();
        return { result.type, std::nullopt, std::move(foundChild) };
    }

    if (m_keyCount >= MinKeys || m_index == 1)
    {
        Flush();
        return { DeleteResult::Type::Deleted, std::nullopt, nullptr };
    }

    std::unique_ptr<Node> leftSiblingNode;
    std::unique_ptr<Node> rightSiblingNode;

    if (leftSibling)
    {
        leftSiblingNode = std::make_unique<Node>(m_dir, leftSibling->index);
        leftSiblingNode->Load();

        if (leftSiblingNode->m_keyCount > MinKeys)
        {
            auto key = leftSiblingNode->GetLastKey();
            auto ptr = leftSiblingNode->m_ptrs[leftSiblingNode->m_keyCount];
            RemoveFromArray(leftSiblingNode->m_keys, leftSiblingNode->m_keyCount - 1);
            RemoveFromArray(leftSiblingNode->m_ptrs, leftSiblingNode->m_keyCount);
            leftSiblingNode->m_keyCount--;
            leftSiblingNode->Flush();

            InsertToArray(m_keys, 0, leftSibling->key);
            InsertToArray(m_ptrs, 0, ptr);
            m_keyCount++;
            Flush();

            return { DeleteResult::Type::BorrowedLeft, GetMinimum() };
        }
    }

    if (rightSibling)
    {
        rightSiblingNode = std::make_unique<Node>(m_dir, rightSibling->index);
        rightSiblingNode->Load();

        if (rightSiblingNode->m_keyCount > MinKeys)
        {
            auto key = rightSiblingNode->m_keys[0];
            auto ptr = rightSiblingNode->m_ptrs[0];
            RemoveFromArray(rightSiblingNode->m_keys, 0);
            RemoveFromArray(rightSiblingNode->m_ptrs, 0);
            rightSiblingNode->m_keyCount--;
            rightSiblingNode->Flush();
            InsertToArray(m_keys, m_keyCount, rightSibling->key);
            InsertToArray(m_ptrs, m_keyCount + 1, ptr);
            m_keyCount++;
            Flush();
            return { DeleteResult::Type::BorrowedRight, rightSiblingNode->GetMinimum() };
        }
    }

    if (leftSiblingNode)
    {
        if (leftSibling->key != key)
            InsertToSortedArray(m_keys, m_keyCount, leftSibling->key);
        else
            InsertToSortedArray(m_keys, m_keyCount, *result.key);
        m_keyCount++;

        std::array<Key, MaxKeys> newKeys = leftSiblingNode->m_keys;
        std::array<Key, B> newPtrs = leftSiblingNode->m_ptrs;
        for (uint32_t i = leftSiblingNode->m_keyCount; i <= m_keyCount + leftSiblingNode->m_keyCount - 1; i++)
        {
            newKeys[i] = m_keys[i - leftSiblingNode->m_keyCount];
            newPtrs[i + 1] = m_ptrs[i - leftSiblingNode->m_keyCount];
        }

        m_keys = std::move(newKeys);
        m_ptrs = std::move(newPtrs);

        m_keyCount += leftSiblingNode->m_keyCount;

        Flush();
        Remove(m_dir, leftSiblingNode->GetIndex());

        return { DeleteResult::Type::MergedLeft, GetMinimum() };
    }
    else if (rightSiblingNode)
    {
        if (rightSibling->key != key)
            InsertToSortedArray(m_keys, m_keyCount, rightSibling->key);
        else
            InsertToSortedArray(m_keys, m_keyCount, *result.key);
        m_keyCount++;

        for (uint32_t i = m_keyCount; i <= m_keyCount + rightSiblingNode->m_keyCount - 1; i++)
        {
            m_keys[i] = rightSiblingNode->m_keys[i - m_keyCount];
            m_ptrs[i] = rightSiblingNode->m_ptrs[i - m_keyCount];
        }
        m_keyCount += rightSiblingNode->m_keyCount;
        m_ptrs[m_keyCount] = rightSiblingNode->m_ptrs[rightSiblingNode->m_keyCount];

        Flush();
        Remove(m_dir, rightSiblingNode->GetIndex());
        return { DeleteResult::Type::MergedRight, GetMinimum() };
    }
    else
    {
        throw std::runtime_error("Bad tree status");
    }
}

Key Node::GetMinimum() const
{
    auto child = CreateBPNode(m_dir, m_ptrs[0]);
    return child->GetMinimum();
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
    in.read(reinterpret_cast<char*>(&(m_ptrs)), sizeof(m_ptrs));
}


void Node::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("8", 1);

    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    out.write(reinterpret_cast<char*>(&(m_ptrs)), sizeof(m_ptrs));
}

} // kv_storage
