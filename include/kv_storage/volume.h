#ifndef KV_STORAGE_VOLUME_H
#define KV_STORAGE_VOLUME_H

#include <string>
#include <memory>
#include <filesystem>
#include <unordered_map>

#include <kv_storage/detail/node.h>
#include <kv_storage/detail/keys_deleter.h>

namespace fs = std::filesystem;

namespace kv_storage {

//-------------------------------------------------------------------------------
//                            VolumeEnumerator
//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
class VolumeEnumerator
{
public:
    VolumeEnumerator(const fs::path& directory, std::weak_ptr<BPCache<V, BranchFactor>> cache, std::shared_ptr<BPNode<V, BranchFactor>> firstBatch, boost::shared_lock<boost::shared_mutex>&& lock);
    virtual bool MoveNext();
    virtual std::pair<Key, V> GetCurrent();
    virtual ~VolumeEnumerator() = default;

private:
    std::shared_ptr<Leaf<V, BranchFactor>> m_currentBatch;
    int32_t m_counter{ -1 };
    const fs::path m_dir;
    std::weak_ptr<BPCache<V, BranchFactor>> m_cache;
    bool m_isValid{ true };
    boost::shared_lock<boost::shared_mutex> m_lock;
};

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
VolumeEnumerator<V, BranchFactor>::VolumeEnumerator(const fs::path& directory, std::weak_ptr<BPCache<V, BranchFactor>> cache, std::shared_ptr<BPNode<V, BranchFactor>> firstBatch, boost::shared_lock<boost::shared_mutex>&& lock)
    : m_dir(directory)
    , m_cache(cache)
    , m_currentBatch(std::static_pointer_cast<Leaf<V, BranchFactor>>(firstBatch))
    , m_lock(std::move(lock))
{
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
bool VolumeEnumerator<V, BranchFactor>::MoveNext()
{
    if (!m_isValid)
        return false;

    m_counter++;
    if (m_counter == m_currentBatch->GetKeyCount())
    {
        if (!m_currentBatch->m_nextBatch)
        {
            m_isValid = false;
            return false;
        }

        auto nextBatch = m_currentBatch->m_nextBatch;

        m_currentBatch = std::static_pointer_cast<Leaf<V, BranchFactor>>(CreateBPNode<V>(m_dir, m_cache, nextBatch));
        m_counter = 0;
        return true;
    }
    else
    {
        return true;
    }
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::pair<Key, V> VolumeEnumerator<V, BranchFactor>::GetCurrent()
{
    return { m_currentBatch->m_keys[m_counter], m_currentBatch->m_values[m_counter] };
}

//-------------------------------------------------------------------------------
//                                   Volume
//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor = 150>
class Volume
{
public:
    Volume(const fs::path& directory, size_t cacheSize = 200000);

    Volume(Volume&&) noexcept;
    Volume& operator= (Volume&&) noexcept;

    void Put(const Key& key, const V& value, std::optional<uint32_t> keyTtl = std::nullopt);
    std::optional<V> Get(const Key& key) const;
    void Delete(const Key& key);
    std::shared_ptr<BPNode<V, BranchFactor>> GetCustomNode(FileIndex idx) const;
    std::unique_ptr<VolumeEnumerator<V, BranchFactor>> Enumerate() const;
    void StopAndFlush();
    ~Volume();

    void StartAutoDelete();

private:
    std::unique_ptr<OutdatedKeysDeleter<V, BranchFactor>> m_deleter;
    std::shared_ptr<BPNode<V, BranchFactor>> m_root;
    const fs::path m_dir;
    mutable std::shared_ptr<BPCache<V, BranchFactor>> m_cache;
    IndexManager m_indexManager;
    mutable boost::shared_mutex m_mutex;
};

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
Volume<V, BranchFactor>::Volume(Volume<V, BranchFactor>&& other) noexcept
    : m_deleter(std::move(other.m_deleter))
    , m_root(std::move(other.m_root))
    , m_dir(std::move(other.m_dir))
    , m_cache(std::move(other.m_cache))
    , m_indexManager(m_dir)
{}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
Volume<V, BranchFactor>& Volume<V, BranchFactor>::operator= (Volume<V, BranchFactor>&& other) noexcept
{
    m_deleter = std::move(m_deleter);
    m_root = std::move(other.m_root);
    m_dir = std::move(other.m_dir);
    m_cache = std::move(other.m_cache);
    m_indexManager = IndexManager(m_dir);
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Volume<V, BranchFactor>::StopAndFlush()
{
    m_deleter->Stop();
    m_deleter->Flush();
    m_root->Flush();
    m_cache->clear();
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
Volume<V, BranchFactor>::~Volume()
{
    try
    {
        StopAndFlush();
    }
    catch (...)
    {
    }
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Volume<V, BranchFactor>::StartAutoDelete()
{
    m_deleter = std::make_unique<OutdatedKeysDeleter<V, BranchFactor>>(this, m_dir);
    m_deleter->Start();
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::shared_ptr<BPNode<V, BranchFactor>> Volume<V, BranchFactor>::GetCustomNode(FileIndex idx) const
{
    if (idx == 1)
        return m_root;
    
    return CreateBPNode(m_dir, std::weak_ptr<BPCache<V, BranchFactor>>(m_cache), idx);
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Volume<V, BranchFactor>::Put(const Key& key, const V& value, std::optional<uint32_t> keyTtl /*= std::nullopt*/)
{
    std::vector<boost::upgrade_lock<boost::shared_mutex>> locks;

    locks.emplace_back(m_mutex);

    auto current = m_root;

    constexpr auto MaxKeys = BranchFactor - 1;

    std::vector<std::shared_ptr<Node<V, BranchFactor>>> nodes;

    locks.emplace_back(current->m_mutex);

    // Searching leaf for insert, lock nodes and save processed nodes
    while (!current->IsLeaf())
    {
        auto currentNode = std::static_pointer_cast<Node<V, BranchFactor>>(current);
        nodes.push_back(currentNode);

        auto child = currentNode->GetChildByKey(key);

        boost::upgrade_lock<boost::shared_mutex> lock(child->m_mutex);

        current = child;

        if (current->GetKeyCount() < MaxKeys)
        {
            // Current node is safe - we can release the ancestor nodes
            locks.clear();
        }

        locks.push_back(std::move(lock));
    }

    // Upgrading locks to unique mode
    std::vector<boost::upgrade_to_unique_lock<boost::shared_mutex>> exclusiveLocks;
    for (auto& l : locks)
    {
        exclusiveLocks.emplace_back(l);
    }

    // Put to the leaf
    auto leaf = std::static_pointer_cast<Leaf<V, BranchFactor>>(current);
    std::optional<CreatedBPNode<V, BranchFactor>> newNode = leaf->Put(key, value, m_indexManager);

    // If child node has been splitted than we should link a new node to parent. Repeat while nodes is splitting
    auto nodesIt = nodes.rbegin();
    while (newNode && nodesIt != nodes.rend())
    {
        newNode = (*nodesIt)->Put(key, newNode.value(), m_indexManager);
        nodesIt++;
    }

    if (!newNode)
    {
        while (!exclusiveLocks.empty())
        {
            exclusiveLocks.pop_back();
        }
        locks.clear();

        if (keyTtl && m_deleter)
            m_deleter->Put(key, keyTtl.value());

        return;
    }

    // Splitting the root
    std::array<Key, MaxKeys> keys;
    keys.fill(0);
    std::array<FileIndex, BranchFactor> ptrs;
    ptrs.fill(0);

    keys[0] = newNode.value().key;
    ptrs[0] = m_root->GetIndex();
    ptrs[1] = newNode.value().node->GetIndex();

    m_cache->insert(m_root->GetIndex(), m_root);

    m_root = std::make_unique<Node<V, BranchFactor>>(m_dir, m_cache, 1, 1, std::move(keys), std::move(ptrs));
    m_cache->insert(1, m_root);

    while (!exclusiveLocks.empty())
    {
        exclusiveLocks.pop_back();
    }
    locks.clear();

    if (keyTtl && m_deleter)
        m_deleter->Put(key, keyTtl.value());
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::optional<V> Volume<V, BranchFactor>::Get(const Key& key) const
{
    auto current = m_root;
    auto firstLock = std::make_unique<boost::shared_lock<boost::shared_mutex>>(current->m_mutex);
    std::unique_ptr<boost::shared_lock<boost::shared_mutex>> secondLock;

    while (true)
    {
        if (!current->IsLeaf())
        {
            auto currentNode = std::static_pointer_cast<Node<V, BranchFactor>>(current);

            auto child = currentNode->GetChildByKey(key);

            secondLock = std::make_unique<boost::shared_lock<boost::shared_mutex>>(child->m_mutex);
            firstLock = std::move(secondLock);

            current = child;
        }
        else
        {
            break;
        }
    }

    return current->Get(key);
}

struct Sibling;

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Volume<V, BranchFactor>::Delete(const Key& key)
{
    std::vector<boost::upgrade_lock<boost::shared_mutex>> locks;

    locks.emplace_back(m_mutex);

    auto current = m_root;

    std::vector<std::tuple<std::shared_ptr<Node<V, BranchFactor>>, std::optional<Sibling>, std::optional<Sibling>, uint32_t>> nodes;

    locks.emplace_back(current->m_mutex);

    // Searching the leaf for deleting with locking mutexes and saving some metainformation like siblings and child position.
    while (!current->IsLeaf())
    {
        auto currentNode = std::static_pointer_cast<Node<V, BranchFactor>>(current);

        std::optional<Sibling> left;
        std::optional<Sibling> right;
        uint32_t childPos = 0;

        auto child = currentNode->GetChildByKey(key, left, right, childPos);

        nodes.emplace_back(currentNode, left, right, childPos);

        boost::upgrade_lock<boost::shared_mutex> lock(child->m_mutex);

        current = child;

        if (current->GetKeyCount() > Half(BranchFactor))
        {
            // Current node is safe - we can release the ancestor nodes
            locks.clear();
        }

        locks.push_back(std::move(lock));
    }

    // Upgrading locks to unique mode
    std::vector<boost::upgrade_to_unique_lock<boost::shared_mutex>> exclusiveLocks;
    for (auto& l : locks)
    {
        exclusiveLocks.emplace_back(l);
    }

    auto leaf = std::static_pointer_cast<Leaf<V, BranchFactor>>(current);

    auto nodesIt = nodes.rbegin();

    std::optional<Sibling> leftSibling;
    std::optional<Sibling> rightSibling;
    if (nodesIt != nodes.rend())
    {
        leftSibling = std::get<1>(nodes.back());
        rightSibling = std::get<2>(nodes.back());
    }

    // Delete from the leaf and save delete result
    auto deleteResult = leaf->Delete(key, leftSibling, rightSibling, m_indexManager);

    auto counter = exclusiveLocks.size() - 1;

    while (nodesIt != nodes.rend() && counter)
    {
        auto node = std::get<0>(*nodesIt);
        std::shared_ptr<BPNode<V, BranchFactor>> child;
        if (nodesIt == nodes.rbegin())
        {
            child = current;
        }
        else
        {
            child = std::get<0>(*std::prev(nodesIt));
        }
        auto childPos = std::get<3>(*nodesIt);

        std::optional<Sibling> left;
        std::optional<Sibling> right;

        auto siblingsIt = std::next(nodesIt);
        if (siblingsIt != nodes.rend())
        {
            left = std::get<1>(*siblingsIt);
            right = std::get<2>(*siblingsIt);
        }

        deleteResult = node->Delete(key, left, right, deleteResult, childPos, child, m_indexManager);
        nodesIt++;
        counter--;
    }

    if (!counter)
    {
        while (!exclusiveLocks.empty())
        {
            exclusiveLocks.pop_back();
        }
        locks.clear();

        if (m_deleter)
            m_deleter->Delete(key);

        return;
    }

    // Special case when height of tree is decreasing. We should replace root node.
    if (deleteResult.type == DeleteType::MergedRight || deleteResult.type == DeleteType::MergedLeft)
    {
        m_root = std::move(deleteResult.node);
        m_cache->insert(1, m_root);
    }

    while (!exclusiveLocks.empty())
    {
        exclusiveLocks.pop_back();
    }
    locks.clear();

    if (m_deleter)
        m_deleter->Delete(key);

    return;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
Volume<V, BranchFactor>::Volume(const fs::path& directory, size_t cacheSize)
    : m_dir(directory)
    , m_cache(std::make_shared<BPCache<V, BranchFactor>>(cacheSize
        , [](std::shared_ptr<BPNode<V, BranchFactor>>& node) { node->Flush(); }))
    , m_indexManager(m_dir)
{
    if (!fs::exists(m_dir / "batch_1.dat"))
    {
        fs::create_directories(m_dir);
        m_root = CreateEmptyBPNode(m_dir, std::weak_ptr<BPCache<V, BranchFactor>>(m_cache), 1);
    }
    else
    {
        m_root = CreateBPNode<V, BranchFactor>(m_dir, m_cache, 1);
    }
    m_cache->insert(1, m_root);
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::unique_ptr<VolumeEnumerator<V, BranchFactor>> Volume<V, BranchFactor>::Enumerate() const
{
    boost::shared_lock<boost::shared_mutex> lock(m_mutex);
    return std::make_unique<VolumeEnumerator<V, BranchFactor>>(m_dir, m_cache, m_root->GetFirstLeaf(), std::move(lock));
}

} // kv_storage

#endif // KV_STORAGE_VOLUME_H
