#ifndef KV_STORAGE_VOLUME_H
#define KV_STORAGE_VOLUME_H

#include <string>
#include <memory>
#include <filesystem>

#include "../src/node.h"

namespace fs = std::filesystem;

namespace kv_storage {

template<class V>
class BPNode;

template <class V>
struct DeleteResult;

struct Sibling;

template<class V>
std::shared_ptr<BPNode<V>> CreateEmptyBPNode(const fs::path& dir, std::weak_ptr<BPCache<V>> cache, FileIndex idx)
{
    auto leaf = std::make_shared<Leaf<V>>(dir, cache, idx);
    cache.lock()->insert(idx, leaf);
    return leaf;
}

template<class V>
std::shared_ptr<BPNode<V>> CreateBPNode(const fs::path& dir, std::weak_ptr<BPCache<V>> cache, FileIndex idx)
{
    auto bpNode = cache.lock()->get(idx);
    if (bpNode)
        return *bpNode;

    std::ifstream in;
    in.exceptions(~std::ifstream::goodbit);
    in.open(fs::path(dir) / ("batch_" + std::to_string(idx) + ".dat"), std::ios::in | std::ios::binary);

    char type;
    in.read(&type, 1);

    // TODO: reuse ifstream instead of closing
    if (type == '8')
    {
        in.close();
        auto node = std::make_shared<Node<V>>(dir, cache, idx);
        node->Load();
        cache.lock()->insert(idx, node);
        return node;
    }
    else if (type == '9')
    {
        in.close();
        auto leaf = std::make_shared<Leaf<V>>(dir, cache, idx);
        leaf->Load();
        cache.lock()->insert(idx, leaf);
        return leaf;
    }
    else
    {
        throw std::runtime_error("Invalid file format");
    }
}

template <class V>
class VolumeEnumerator
{
public:
    VolumeEnumerator(const fs::path& directory, std::weak_ptr<BPCache<V>> cache, std::shared_ptr<BPNode<V>> firstBatch);
    virtual bool MoveNext();
    virtual std::pair<Key, V> GetCurrent();
    virtual ~VolumeEnumerator() = default;

private:
    std::shared_ptr<Leaf<V>> m_currentBatch;
    int32_t m_counter{ -1 };
    const fs::path m_dir;
    std::weak_ptr<BPCache<V>> m_cache;
    bool isValid{ true };
};

template <class V>
class Volume
{
public:
    Volume(const fs::path& directory);

    virtual void Put(const Key& key, const V& value);
    virtual std::optional<V> Get(const Key& key) const;
    virtual void Delete(const Key& key);
    virtual std::shared_ptr<BPNode<V>> Volume<V>::GetCustomNode(FileIndex idx) const;
    virtual std::unique_ptr<VolumeEnumerator<V>> Enumerate() const;
    virtual ~Volume() = default;

private:
    std::shared_ptr<BPNode<V>> m_root;
    const fs::path m_dir;
    FileIndex m_nodesCount;
    mutable std::shared_ptr<BPCache<V>> m_cache;
};

template<class V>
std::pair<Key, V> VolumeEnumerator<V>::GetCurrent()
{
    return { m_currentBatch->m_keys[m_counter], m_currentBatch->m_values[m_counter] };
}

template<class V>
std::shared_ptr<BPNode<V>> Volume<V>::GetCustomNode(FileIndex idx) const
{
    if (idx == 1)
        return m_root;
    
    return CreateBPNode(m_dir, std::weak_ptr<BPCache<V>>(m_cache), idx);
}

template<class V>
void Volume<V>::Put(const Key& key, const V& value)
{
    auto newNode = m_root->Put(key, value, m_nodesCount);
    if (!newNode)
        return;

    std::array<Key, MaxKeys> keys;
    keys.fill(0);
    std::array<FileIndex, B> ptrs;
    ptrs.fill(0);

    keys[0] = newNode.value().key;
    ptrs[0] = m_root->GetIndex();
    ptrs[1] = newNode.value().node->GetIndex();

    m_cache->insert(m_root->GetIndex(), m_root);

    m_root = std::make_unique<Node<V>>(m_dir, m_cache, 1, 1, std::move(keys), std::move(ptrs));
    m_cache->insert(1, m_root);
}

template<class V>
std::optional<V> Volume<V>::Get(const Key& key) const
{
    return m_root->Get(key);
}

template<class V>
VolumeEnumerator<V>::VolumeEnumerator(const fs::path& directory, std::weak_ptr<BPCache<V>> cache, std::shared_ptr<BPNode<V>> firstBatch)
    : m_dir(directory)
    , m_cache(cache)
    , m_currentBatch(std::static_pointer_cast<Leaf<V>>(firstBatch))
{}

template<class V>
bool VolumeEnumerator<V>::MoveNext()
{
    if (!isValid)
        return false;

    m_counter++;
    if (m_counter == m_currentBatch->GetKeyCount())
    {
        if (!m_currentBatch->m_nextBatch)
        {
            isValid = false;
            return false;
        }

        auto nextBatch = m_currentBatch->m_nextBatch;

        m_currentBatch = std::static_pointer_cast<Leaf<V>>(CreateBPNode<V>(m_dir, m_cache, nextBatch));
        m_counter = 0;
        return true;
    }
    else
    {
        return true;
    }
}

template<class V>
void Volume<V>::Delete(const Key& key)
{
    auto res = m_root->Delete(key, std::nullopt, std::nullopt);
    if (res.type == DeleteType::MergedRight || res.type == DeleteType::MergedLeft)
    {
        m_root = std::move(res.node);
        m_cache->insert(1, m_root);
        return;
    }
}

template<class V>
Volume<V>::Volume(const fs::path& directory)
    : m_dir(directory)
    , m_cache(std::make_shared<BPCache<V>>(5000))
{
    if (!fs::exists(m_dir / "batch_1.dat"))
    {
        fs::create_directories(m_dir);
        m_root = CreateEmptyBPNode(m_dir, std::weak_ptr<BPCache<V>>(m_cache), 1);
    }
    else
    {
        m_root = CreateBPNode<V>(m_dir, m_cache, 1);
    }
    m_cache->insert(1, m_root);
    m_nodesCount = 1;
}

template<class V>
std::unique_ptr<VolumeEnumerator<V>> Volume<V>::Enumerate() const
{
    return std::make_unique<VolumeEnumerator<V>>(m_dir, m_cache, m_root->GetFirstLeaf());
}

} // kv_storage

#endif // KV_STORAGE_VOLUME_H
