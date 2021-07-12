#ifndef LEAF_H
#define LEAF_H

#include "bp_node.h"

namespace kv_storage {

using Offset = uint64_t;
class Leaf : public BPNode
{
public:
    Leaf(const fs::path& dir, FileIndex idx)
        : BPNode(dir, idx)
    {
        valuesOffsets.fill(0);
    }

    Leaf(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<Offset, MaxKeys>&& newOffsets, std::vector<std::string>&& newValues, FileIndex newNextBatch)
        : BPNode(dir, idx, newKeyCount, std::move(newKeys))
        , valuesOffsets(newOffsets)
        , values(newValues)
        , m_nextBatch(newNextBatch)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& val, FileIndex& nodesCount) override;
    CreatedBPNode SplitAndPut(Key key, const std::string& value, FileIndex& nodesCount);
    std::string Get(Key key) const override;

private:
    std::array<Offset, MaxKeys> valuesOffsets;
    std::vector<std::string> values;
    FileIndex m_nextBatch{ 0 };
};

} // kv_storage

#endif // LEAF_H
