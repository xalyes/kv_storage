#ifndef LEAF_H
#define LEAF_H

#include <fstream>
#include <optional>
#include <type_traits>

#include "bp_node.h"

namespace kv_storage {

//-------------------------------------------------------------------------------
//                                  Leaf
//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
class Leaf
    : public BPNode<V, BranchFactor>
    , public std::enable_shared_from_this<BPNode<V, BranchFactor>>
{
public:
    template<class, size_t> friend class VolumeEnumerator;

    Leaf(const fs::path& dir, std::weak_ptr<BPCache<V, BranchFactor>> cache, FileIndex idx)
        : BPNode<V, BranchFactor>(dir, cache, idx)
    {
    }

    Leaf(const fs::path& dir, std::weak_ptr<BPCache<V, BranchFactor>> cache, FileIndex idx, uint32_t newKeyCount, std::array<Key, BranchFactor - 1>&& newKeys, std::vector<V>&& newValues, FileIndex newNextBatch)
        : BPNode<V, BranchFactor>(dir, cache, idx, newKeyCount, std::move(newKeys))
        , m_values(newValues)
        , m_nextBatch(newNextBatch)
    {}

    virtual ~Leaf();
    virtual void Flush() override;
    virtual void Load() override;
    virtual std::optional<V> Get(Key key) const override;
    virtual Key GetMinimum() const override;
    virtual std::shared_ptr<BPNode<V, BranchFactor>> GetFirstLeaf() override;
    virtual bool IsLeaf() const override;

    DeleteResult<V, BranchFactor> Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling, IndexManager& indexManager);
    std::optional<CreatedBPNode<V, BranchFactor>> Put(Key key, const V& val, IndexManager& indexManager);

private:
    CreatedBPNode<V, BranchFactor> SplitAndPut(Key key, const V& value, IndexManager& indexManager);
    void LeftJoin(const Leaf<V, BranchFactor>& leaf);
    void RightJoin(const Leaf<V, BranchFactor>& leaf);
    void Insert(Key key, const V& value, uint32_t pos);

    using std::enable_shared_from_this<BPNode<V, BranchFactor>>::shared_from_this;
    using BPNode<V, BranchFactor>::m_keyCount;
    using BPNode<V, BranchFactor>::m_keys;
    using BPNode<V, BranchFactor>::m_dirty;
    using BPNode<V, BranchFactor>::m_index;
    using BPNode<V, BranchFactor>::m_dir;
    using BPNode<V, BranchFactor>::m_cache;
    using BPNode<V, BranchFactor>::m_mutex;

    template<class T>
    typename std::enable_if<
        std::is_same<T, float>::value
     || std::is_same<T, uint32_t>::value
     || std::is_same<T, uint64_t>::value
     || std::is_same<T, double>::value, void>::type
        ReadValues(std::ifstream& in)
    {
        uint32_t sz = static_cast<uint32_t>(sizeof(T)) * m_keyCount;
        m_values.resize(sz);
        in.read(reinterpret_cast<char*>(m_values.data()), sz);

        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            LittleToNativeEndianInplace(m_values[i]);
        }
    }

    template<class T>
    typename std::enable_if<
        std::is_same<T, std::string>::value, void>::type
        ReadValues(std::ifstream& in)
    {
        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            uint32_t size;
            in.read(reinterpret_cast<char*>(&size), sizeof(size));
            boost::endian::little_to_native_inplace(size);

            auto buf = std::make_unique<char[]>(size + 1);
            in.read(buf.get(), size);
            buf.get()[size] = '\0';
            m_values.emplace_back(buf.get());
        }
    }

    template<class T>
    typename std::enable_if<
        std::is_same<T, std::vector<char>>::value, void>::type
        ReadValues(std::ifstream& in)
    {
        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            uint32_t size;
            in.read(reinterpret_cast<char*>(&size), sizeof(size));
            boost::endian::little_to_native_inplace(size);

            auto buf = std::make_unique<char[]>(size);
            in.read(buf.get(), size);
            m_values.emplace_back(buf.get(), buf.get() + size);
        }
    }

    template<class T>
    typename std::enable_if<
        std::is_same<T, float>::value
     || std::is_same<T, uint32_t>::value
     || std::is_same<T, uint64_t>::value
     || std::is_same<T, double>::value, void>::type
        WriteValues(std::ofstream& out)
    {
        for (uint32_t i = 0; i < m_values.size(); i++)
        {
            auto val = NativeToLittleEndian(m_values[i]);
            out.write(reinterpret_cast<char*>(&val), sizeof(val));
        }
    }

    template<class T>
    typename std::enable_if<
        std::is_same<T, std::string>::value
     || std::is_same<T, std::vector<char>>::value, void>::type
        WriteValues(std::ofstream& out)
    {
        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            uint32_t size = boost::endian::native_to_little(static_cast<uint32_t>(m_values[i].size()));
            out.write(reinterpret_cast<char*>(&size), sizeof(size));
            out.write(m_values[i].data(), m_values[i].size());
        }
    }

private:
    std::vector<V> m_values;
    FileIndex m_nextBatch{ 0 };
};

//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
Leaf<V, BranchFactor>::~Leaf()
{
    try
    {
        Flush();
    }
    catch (...)
    {
    }
}

//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
bool Leaf<V, BranchFactor>::IsLeaf() const
{
    return true;
}

//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
std::shared_ptr<BPNode<V, BranchFactor>> Leaf<V, BranchFactor>::GetFirstLeaf()
{
    return shared_from_this();
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Leaf<V, BranchFactor>::Insert(Key key, const V& value, uint32_t pos)
{
    InsertToArray(m_keys, pos, key);
    m_values.insert(m_values.begin() + pos, value);
    m_keyCount++;
    m_dirty = true;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::optional<CreatedBPNode<V, BranchFactor>> Leaf<V, BranchFactor>::Put(Key key, const V& val, IndexManager& indexManager)
{
    constexpr auto MaxKeys = BranchFactor - 1;
    if (m_keyCount == MaxKeys)
    {
        if (m_index == 1)
        {
            m_index = indexManager.FindFreeIndex(m_dir);
        }

        return SplitAndPut(key, val, indexManager);
    }

    if (m_keyCount == 0)
    {
        Insert(key, val, 0);
    }
    else
    {
        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            auto currentKey = m_keys[i];

            if (key < currentKey)
            {
                if (m_keyCount != MaxKeys)
                {
                    Insert(key, val, i);
                }
                else
                {
                    throw std::runtime_error("Try to insert value to fullfilled Leaf");
                }

                return std::nullopt;
            }

            if (currentKey == key)
            {
                throw std::runtime_error("Couldn't insert exising key");
            }
        }

        Insert(key, val, m_keyCount);
    }

    return std::nullopt;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
CreatedBPNode<V, BranchFactor> Leaf<V, BranchFactor>::SplitAndPut(Key key, const V& value, IndexManager& indexManager)
{
    constexpr auto MaxKeys = BranchFactor - 1;

    uint32_t copyCount = MaxKeys / 2;

    std::array<Key, MaxKeys> newKeys;
    newKeys.fill(0);

    std::vector<V> newValues;

    uint32_t borderIndex = static_cast<uint32_t>(m_values.size()) - copyCount;

    newValues.insert(newValues.end(), std::make_move_iterator(m_values.begin() + borderIndex),
        std::make_move_iterator(m_values.end()));
    m_values.erase(m_values.begin() + borderIndex, m_values.end());

    std::swap(m_keys[borderIndex], newKeys[0]);

    for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
    {
        std::swap(m_keys[i], newKeys[i - borderIndex]);
    }

    m_keyCount -= copyCount;

    Key firstNewKey = newKeys[0];

    auto nodesCount = indexManager.FindFreeIndex(m_dir);
    auto newLeaf = std::make_shared<Leaf>(m_dir, m_cache, nodesCount, copyCount, std::move(newKeys), std::move(newValues), m_nextBatch);

    m_nextBatch = newLeaf->m_index;

    if (key < firstNewKey)
    {
        Put(key, value, indexManager);
    }
    else
    {
        newLeaf->Put(key, value, indexManager);
    }

    m_cache.lock()->insert(nodesCount, newLeaf);
    return { std::move(newLeaf), firstNewKey };
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::optional<V> Leaf<V, BranchFactor>::Get(Key key) const
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            return m_values[i];
        }
    }

    return std::nullopt;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Leaf<V, BranchFactor>::LeftJoin(const Leaf<V, BranchFactor>& leaf)
{
    std::array<Key, BranchFactor - 1> newKeys = leaf.m_keys;

    for (uint32_t i = leaf.m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        newKeys[i] = m_keys[i - leaf.m_keyCount];
    }
    m_keys = std::move(newKeys);
    m_values.insert(m_values.begin(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
    m_index = leaf.m_index;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Leaf<V, BranchFactor>::RightJoin(const Leaf<V, BranchFactor>& leaf)
{
    for (uint32_t i = m_keyCount; i <= m_keyCount + leaf.m_keyCount - 1; i++)
    {
        m_keys[i] = leaf.m_keys[i - m_keyCount];
    }

    m_values.insert(m_values.end(), leaf.m_values.begin(), leaf.m_values.end());
    m_keyCount += leaf.m_keyCount;
    m_nextBatch = leaf.m_nextBatch;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
std::shared_ptr<BPNode<V, BranchFactor>> CreateBPNode(const fs::path& dir, BPCache<V, BranchFactor>& cache, FileIndex idx);

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
DeleteResult<V, BranchFactor> Leaf<V, BranchFactor>::Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling, IndexManager& indexManager)
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            // 1. First of all remove key and value.
            RemoveFromArray(m_keys, i);
            m_values.erase(m_values.begin() + i);
            m_keyCount--;
            m_dirty = true;

            // 2. Check key count.
            // If we have too few keys and this leaf is not root we should make some additional changes.
            if (m_index == 1 || m_keyCount >= Half(BranchFactor))
            {
                return { DeleteType::Deleted, std::nullopt };
            }

            std::shared_ptr<Leaf> leftSiblingLeaf;
            std::shared_ptr<Leaf> rightSiblingLeaf;

            // 3. If left sibling has enough keys we can simple borrow the entry.
            if (leftSibling)
            {
                leftSiblingLeaf = std::static_pointer_cast<Leaf<V, BranchFactor>>(CreateBPNode<V, BranchFactor>(m_dir, m_cache, leftSibling->index));

                if (leftSiblingLeaf->m_keyCount > Half(BranchFactor))
                {
                    auto key = leftSiblingLeaf->GetLastKey();
                    auto value = leftSiblingLeaf->m_values[leftSiblingLeaf->m_keyCount - 1];
                    Insert(key, value, 0);
                    leftSiblingLeaf->Delete(key, std::nullopt, std::nullopt, indexManager);
                    return { DeleteType::BorrowedLeft, m_keys[0] };
                }
            }

            // 4. If right sibling has enough keys we can simple borrow the entry.
            if (rightSibling)
            {
                rightSiblingLeaf = std::static_pointer_cast<Leaf>(CreateBPNode<V, BranchFactor>(m_dir, m_cache, rightSibling->index));

                if (rightSiblingLeaf->m_keyCount > Half(BranchFactor))
                {
                    auto key = rightSiblingLeaf->m_keys[0];
                    auto value = rightSiblingLeaf->m_values[0];
                    Insert(key, value, m_keyCount);
                    rightSiblingLeaf->Delete(key, std::nullopt, std::nullopt, indexManager);
                    return { DeleteType::BorrowedRight, rightSiblingLeaf->m_keys[0] };
                }
            }

            // 5. Both siblngs have too few keys. We should merge this leaf and sibling.
            if (leftSibling)
            {
                const auto currentIndex = m_index;
                LeftJoin(*leftSiblingLeaf);
                m_cache.lock()->erase(currentIndex);
                indexManager.Remove(m_dir, currentIndex);

                return { DeleteType::MergedLeft, m_keys[0] };
            }
            else if (rightSibling)
            {
                RightJoin(*rightSiblingLeaf);
                m_cache.lock()->erase(rightSiblingLeaf->GetIndex());
                indexManager.Remove(m_dir, rightSiblingLeaf->GetIndex());
                rightSiblingLeaf->MarkAsDeleted();

                return { DeleteType::MergedRight, m_keys[0] };
            }
            else
            {
                throw std::runtime_error("Bad leaf status");
            }
        }
    }

    throw std::runtime_error("Failed to remove unexisted value of key '" + std::to_string(key) + "'");
}

//-------------------------------------------------------------------------------
template <class V, size_t BranchFactor>
Key Leaf<V, BranchFactor>::GetMinimum() const
{
    return m_keys[0];
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Leaf<V, BranchFactor>::Flush()
{
    boost::unique_lock<boost::shared_mutex> lock(m_mutex);

    if (!m_dirty)
        return;

    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);

    auto keyCount = boost::endian::native_to_little(m_keyCount);
    out.write(reinterpret_cast<char*>(&keyCount), sizeof(keyCount));

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        auto key = boost::endian::native_to_little(m_keys[i]);
        out.write(reinterpret_cast<char*>(&key), sizeof(key));
    }

    WriteValues<V>(out);

    auto nextBatch = boost::endian::native_to_little(m_nextBatch);
    out.write(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
    out.close();
    m_dirty = false;
}

//-------------------------------------------------------------------------------
template<class V, size_t BranchFactor>
void Leaf<V, BranchFactor>::Load()
{
    boost::unique_lock<boost::shared_mutex> lock(m_mutex);

    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary);

    // TODO: check first char is "9"
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));

    boost::endian::little_to_native_inplace(m_keyCount);

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        boost::endian::little_to_native_inplace(m_keys[i]);
    }

    ReadValues<V>(in);

    in.read(reinterpret_cast<char*>(&(m_nextBatch)), sizeof(m_nextBatch));
    boost::endian::little_to_native_inplace(m_nextBatch);
    m_dirty = false;
}

} // kv_storage

#endif // LEAF_H
