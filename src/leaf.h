#ifndef LEAF_H
#define LEAF_H

#include <optional>

#include "bp_node.h"

namespace kv_storage {

using Offset = uint64_t;

class Leaf : public BPNode
{
public:
    Leaf(const fs::path& dir, BPCache& cache, FileIndex idx)
        : BPNode(dir, cache, idx)
    {
    }

    Leaf(const fs::path& dir, BPCache& cache, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::vector<std::string>&& newValues, FileIndex newNextBatch)
        : BPNode(dir, cache, idx, newKeyCount, std::move(newKeys))
        , m_values(newValues)
        , m_nextBatch(newNextBatch)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& val, FileIndex& nodesCount) override;
    virtual std::string Get(Key key) const override;
    virtual DeleteResult Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling) override;
    virtual Key GetMinimum() const override;

private:
    CreatedBPNode SplitAndPut(Key key, const std::string& value, FileIndex& nodesCount);
    void LeftJoin(const Leaf& leaf);
    void RightJoin(const Leaf& leaf);
    void Insert(Key key, const std::string& value, uint32_t pos);

private:
    std::vector<std::string> m_values;
    FileIndex m_nextBatch{ 0 };
};

} // kv_storage

#endif // LEAF_H
