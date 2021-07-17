#include <fstream>

#include "node.h"
#include "utils.h"

namespace kv_storage {


uint32_t Node::FindKeyPosition(Key key) const
{
    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            return i;
        }
    }

    return m_keyCount;
}

std::string Node::Get(Key key) const
{
    auto foundChild = CreateBPNode(m_dir, m_cache, m_ptrs[FindKeyPosition(key)]);
    return foundChild->Get(key);
}

std::optional<CreatedBPNode> Node::Put(Key key, const std::string& value, FileIndex& nodesCount)
{
    auto foundChild = CreateBPNode(m_dir, m_cache, m_ptrs[FindKeyPosition(key)]);

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
        auto newNode = std::make_shared<Node>(m_dir, m_cache, nodesCount, copyCount, std::move(newKeys), std::move(newPtrs));
        m_cache.insert(nodesCount, newNode);

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
    // 1. Find and load child node/leaf where key could be stored.
    std::shared_ptr<BPNode> foundChild;
    uint32_t childPos;
    {
        childPos = FindKeyPosition(key);
        foundChild = CreateBPNode(m_dir, m_cache, m_ptrs[childPos]);
    }

    // 2. Save information about siblings of found child.
    std::optional<Sibling> childRightSibling;
    std::optional<Sibling> childLeftSibling;
    {
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
    }

    // 3. Recursive call child's delete method
    DeleteResult result = foundChild->Delete(key, childLeftSibling, childRightSibling);

    if (result.type == DeleteResult::Type::Deleted)
    {
        // 4.1 Key deleted.
        // Almost nothing to do. Updating key if need and returning.
        if (childPos && key == m_keys[childPos - 1])
        {
            m_keys[childPos - 1] = foundChild->GetMinimum();
            Flush();
        }
        return result;
    }
    else if (result.type == DeleteResult::Type::BorrowedLeft)
    {
        // 4.2 Key deleted and one key has been borrowed from left sibling.
        // Almost nothing to do. Updating key and returning.
        if (childPos)
            m_keys[childPos - 1] = *result.key;
        else
            throw std::runtime_error("Bad tree status - leftmost child returned BorrowedLeft, but it is impossible.");

        Flush();
        return { DeleteResult::Type::Deleted };
    }
    else if (result.type == DeleteResult::Type::BorrowedRight)
    {
        // 4.3 Key deleted and one key has been borrowed from right sibling.
        // Almost nothing to do. Updating key and returning.
        m_keys[childPos] = *result.key;
        Flush();
        return { DeleteResult::Type::Deleted };
    }
    else if (result.type == DeleteResult::Type::MergedRight)
    {
        // 4.4 Key deleted but right sibling node has been merged with the child node.
        // Removing merged sibling.
        if (childPos > 1)
        {
            m_keys[childPos - 1] = *result.key;
        }

        RemoveFromArray(m_keys, childPos);
        RemoveFromArray(m_ptrs, childPos + 1);
    }
    else // MergedLeft
    {
        // 4.4 Key deleted but left sibling node has been merged with the child node.
        // Removing merged sibling.
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

    // 5. Special handling for situation when this node is root and child node has been merged with sibling.
    // Tree shrinked and child node becomes new root.
    if (m_index == 1 && m_keyCount == 0)
    {
        Remove(m_dir, foundChild->GetIndex());
        foundChild->SetIndex(1);
        foundChild->Flush();
        return { result.type, std::nullopt, std::move(foundChild) };
    }

    // 6. Simple return if this node has enough keys or if it is root.
    if (m_keyCount >= MinKeys || m_index == 1)
    {
        Flush();
        return { DeleteResult::Type::Deleted, std::nullopt, nullptr };
    }

    std::shared_ptr<Node> leftSiblingNode;

    // 7. Try to borrow left sibling's key...
    if (leftSibling)
    {
        leftSiblingNode = std::make_shared<Node>(m_dir, m_cache, leftSibling->index);
        leftSiblingNode->Load();
        m_cache.insert(leftSibling->index, leftSiblingNode);

        if (leftSiblingNode->m_keyCount > MinKeys)
        {
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

    std::shared_ptr<Node> rightSiblingNode;

    // 8. Try to borrow right sibling's key...
    if (rightSibling)
    {
        rightSiblingNode = std::make_shared<Node>(m_dir, m_cache, rightSibling->index);
        rightSiblingNode->Load();
        m_cache.insert(rightSibling->index, rightSiblingNode);

        if (rightSiblingNode->m_keyCount > MinKeys)
        {
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

    // 9. Merging nodes...
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
    auto child = CreateBPNode(m_dir, m_cache, m_ptrs[0]);
    return child->GetMinimum();
}

void Node::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary);

    // TODO: check first char is "8"
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
