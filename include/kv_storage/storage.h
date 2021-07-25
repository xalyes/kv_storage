#ifndef KV_STORAGE_STORAGE_H
#define KV_STORAGE_STORAGE_H

#include <map>

#include "volume.h"

namespace kv_storage {

template<class V>
class StorageNode
{
public:
    StorageNode() {}
    void Mount(const Volume<V>& vol, size_t priority = 0, FileIndex idx = 1);
    std::vector<V> Get(const Key& key) const;
    std::vector<std::shared_ptr<StorageNode<V>>> GetChilds() const;
    std::shared_ptr<StorageNode<V>> CreateChildNode();
    void EraseNode(size_t idx);

private:
    std::multimap<size_t, std::shared_ptr<BPNode<V>>> m_volumeNodes;
    std::vector<std::shared_ptr<StorageNode<V>>> m_childs;
};

template<class V>
std::vector<std::shared_ptr<StorageNode<V>>> StorageNode<V>::GetChilds() const
{
    return m_childs;
}

template<class V>
std::shared_ptr<StorageNode<V>> StorageNode<V>::CreateChildNode()
{
    auto newChild = std::make_shared<StorageNode<V>>(StorageNode<V>());
    m_childs.emplace_back(std::move(newChild));
    return m_childs.back();
}

template<class V>
void StorageNode<V>::EraseNode(size_t idx)
{
    if (m_childs.size() < idx)
        throw std::runtime_error("Failed to delete node - index out of range");

    m_childs.erase(m_childs.begin() + idx);
}

template<class V>
void StorageNode<V>::Mount(const Volume<V>& vol, size_t priority, FileIndex idx)
{
    m_volumeNodes.insert({ priority, vol.GetCustomNode(idx) });
}

template<class V>
std::vector<V> StorageNode<V>::Get(const Key& key) const
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
