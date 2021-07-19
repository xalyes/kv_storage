#ifndef BP_NODE_H
#define BP_NODE_H

#include "utils.h"

namespace kv_storage {

using Key = uint64_t;

constexpr uint32_t Half(uint32_t num)
{
    if (num % 2 == 0)
        return (num / 2) - 1;
    else
        return (num - 1) / 2;
}

constexpr size_t B = 200;
constexpr size_t MaxKeys = B - 1;
constexpr size_t MinKeys = Half(B);

template<class V>
class BPNode;

template<class V>
using BPCache = lru_cache<FileIndex, std::shared_ptr<BPNode<V>>>;

// ptr to new created BPNode & key to be inserted to parent node
template<class V>
struct CreatedBPNode
{
    std::shared_ptr<BPNode<V>> node;
    Key key;
};

enum class DeleteType
{
    Deleted,
    BorrowedRight,
    BorrowedLeft,
    MergedLeft,
    MergedRight
};

template<class V>
struct DeleteResult
{
    DeleteType type;
    std::optional<Key> key;
    std::shared_ptr<BPNode<V>> node;
};

struct Sibling
{
    Key key;
    FileIndex index;
};

template<class V>
class BPNode
{
public:
    BPNode(const fs::path& dir, BPCache<V>& cache, FileIndex idx)
        : m_dir(dir)
        , m_cache(cache)
        , m_index(idx)
    {
        m_keys.fill(0);
    }

    BPNode(const fs::path& dir, BPCache<V>& cache, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys)
        : m_dir(dir)
        , m_cache(cache)
        , m_index(idx)
        , m_keyCount(newKeyCount)
        , m_keys(newKeys)
    {}

    virtual ~BPNode() = default;

    virtual void Load() = 0;
    virtual void Flush() = 0;
    virtual std::optional<CreatedBPNode<V>> Put(Key key, const V& value, FileIndex& nodesCount) = 0;
    virtual std::string Get(Key key) const = 0;
    virtual DeleteResult<V> Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling) = 0;
    virtual std::shared_ptr<BPNode> GetFirstLeaf() = 0;
    virtual Key GetMinimum() const = 0;
    virtual uint32_t GetKeyCount() const;
    virtual Key GetLastKey() const;
    virtual FileIndex GetIndex() const;
    virtual void SetIndex(FileIndex index);

protected:
    const fs::path m_dir;
    BPCache<V>& m_cache;
    FileIndex m_index{ 0 };
    uint32_t m_keyCount{ 0 };
    std::array<Key, MaxKeys> m_keys;
};

template<class V>
uint32_t BPNode<V>::GetKeyCount() const
{
    return m_keyCount;
}

template<class V>
Key BPNode<V>::GetLastKey() const
{
    return m_keys[m_keyCount - 1];
}

template<class V>
FileIndex BPNode<V>::GetIndex() const
{
    return m_index;
}

template<class V>
void BPNode<V>::SetIndex(FileIndex index)
{
    m_index = index;
}

} // kv_storage

#endif // BP_NODE_H
