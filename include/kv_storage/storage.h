#ifndef KV_STORAGE_STORAGE_H
#define KV_STORAGE_STORAGE_H

#include <map>

#include "volume.h"

namespace kv_storage {


//-------------------------------------------------------------------------------
//                              StorageNode
//-------------------------------------------------------------------------------
// StorageNode it is simple node of n-ary tree with mounted volumes or parts 
// of volumes.
//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor = 150>
class StorageNode
{
public:
    StorageNode() {}
    // Mount volume or custom node of volume.
    // vol      - Input parameter. Volume object.
    // priority - Priority of volume/node. This parameter used to choose between values in case of conflicts.
    // idx      - File index in case of mounting part of volume.
    void Mount(const Volume<V>& vol, size_t priority = 0, FileIndex idx = 1);

    // Find key. Return vector of values from another volumes/nodes
    // key - Input parameter. key to be found.
    std::vector<V> Get(const Key& key) const;

    // Method for explore storage tree itself. Get all childs.
    std::vector<std::shared_ptr<StorageNode<V, BranchFactor>>> GetChilds() const;

    // Create new child of node. 
    std::shared_ptr<StorageNode<V, BranchFactor>> CreateChildNode();

    // Erase node from the tree.
    // idx - Input parameter. idx of node in vector of childs.
    void EraseNode(size_t idx);

private:
    std::multimap<size_t, std::shared_ptr<BPNode<V, BranchFactor>>> m_volumeNodes;
    std::vector<std::shared_ptr<StorageNode<V, BranchFactor>>> m_childs;
};

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::vector<std::shared_ptr<StorageNode<V, BranchFactor>>> StorageNode<V, BranchFactor>::GetChilds() const
{
    return m_childs;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::shared_ptr<StorageNode<V, BranchFactor>> StorageNode<V, BranchFactor>::CreateChildNode()
{
    auto newChild = std::make_shared<StorageNode<V, BranchFactor>>(StorageNode<V>());
    m_childs.emplace_back(std::move(newChild));
    return m_childs.back();
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void StorageNode<V, BranchFactor>::EraseNode(size_t idx)
{
    if (m_childs.size() < idx)
        throw std::runtime_error("Failed to delete node - index out of range");

    m_childs.erase(m_childs.begin() + idx);
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void StorageNode<V, BranchFactor>::Mount(const Volume<V>& vol, size_t priority, FileIndex idx)
{
    m_volumeNodes.insert({ priority, vol.GetCustomNode(idx) });
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::vector<V> StorageNode<V, BranchFactor>::Get(const Key& key) const
{
    std::vector<V> values;
    std::optional<V> foundValue;
    for (const auto& vol : m_volumeNodes)
    {
        const auto res = vol.second->Get(key);
        if (res)
        {
            foundValue = res;
            break;
        }
    }

    for (const auto& child : m_childs)
    {
        const auto res = child->Get(key);
        values.insert(values.end(), res.begin(), res.end());
    }
    if (foundValue)
        values.push_back(*foundValue);
    return values;
}

} // kv_storage

#endif // KV_STORAGE_STORAGE_H
