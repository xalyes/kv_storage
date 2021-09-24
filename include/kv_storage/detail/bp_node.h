#ifndef BP_NODE_H
#define BP_NODE_H

#include <optional>

#include "utils.h"

namespace kv_storage {

using Key = uint64_t;

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
class BPNode;

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
using BPCache = lfu_cache<FileIndex, std::shared_ptr<BPNode<V, BranchFactor>>>;

//-------------------------------------------------------------------------------
// ptr to new created BPNode & key to be inserted to parent node
template<class V, size_t BranchFactor>
struct CreatedBPNode
{
    std::shared_ptr<BPNode<V, BranchFactor>> node;
    Key key;
};

//-------------------------------------------------------------------------------
enum class DeleteType
{
    Deleted,
    BorrowedRight,
    BorrowedLeft,
    MergedLeft,
    MergedRight
};

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
struct DeleteResult
{
    DeleteType type;
    std::optional<Key> key;
    std::shared_ptr<BPNode<V, BranchFactor>> node;
};

//-------------------------------------------------------------------------------
struct Sibling
{
    Key key;
    FileIndex index;
};

//-------------------------------------------------------------------------------
//                                BPNode
//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
class BPNode
{
public:
    template<class, size_t> friend class VolumeEnumerator;

    BPNode(const fs::path& dir, std::weak_ptr<BPCache<V, BranchFactor>> cache, FileIndex idx)
        : m_dir(dir)
        , m_cache(cache)
        , m_index(idx)
        , m_dirty(true)
    {
        m_keys.fill(0);
    }

    BPNode(const fs::path& dir, std::weak_ptr<BPCache<V, BranchFactor>> cache, FileIndex idx, uint32_t newKeyCount, std::array<Key, BranchFactor - 1>&& newKeys)
        : m_dir(dir)
        , m_cache(cache)
        , m_index(idx)
        , m_keyCount(newKeyCount)
        , m_keys(newKeys)
        , m_dirty(true)
    {
    }

    virtual ~BPNode() = default;

    virtual void Load() = 0;
    virtual void Flush() = 0;
    virtual std::optional<V> Get(Key key) const = 0;
    virtual std::shared_ptr<BPNode> GetFirstLeaf() = 0;
    virtual Key GetMinimum() const = 0;
    virtual bool IsLeaf() const = 0;

    virtual uint32_t GetKeyCount() const;
    virtual Key GetLastKey() const;
    virtual FileIndex GetIndex() const;
    virtual void SetIndex(FileIndex index);
    virtual void MarkAsDeleted();

    mutable boost::shared_mutex m_mutex;

protected:
    const fs::path m_dir;
    std::weak_ptr<BPCache<V, BranchFactor>> m_cache;
    FileIndex m_index{ 0 };
    uint32_t m_keyCount{ 0 };
    std::array<Key, BranchFactor - 1> m_keys;
    bool m_dirty;
};

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
uint32_t BPNode<V, BranchFactor>::GetKeyCount() const
{
    return m_keyCount;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
Key BPNode<V, BranchFactor>::GetLastKey() const
{
    return m_keys[m_keyCount - 1];
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
FileIndex BPNode<V, BranchFactor>::GetIndex() const
{
    return m_index;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void BPNode<V, BranchFactor>::SetIndex(FileIndex index)
{
    m_index = index;
    m_dirty = true;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void BPNode<V, BranchFactor>::MarkAsDeleted()
{
    m_dirty = false;
}

} // kv_storage

#endif // BP_NODE_H
