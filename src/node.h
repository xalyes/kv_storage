#ifndef NODE_H
#define NODE_H

#include "bp_node.h"

namespace kv_storage {

class Node : public BPNode
{
public:
    Node(const fs::path& dir, FileIndex idx)
        : BPNode(dir, idx)
    {
        ptrs.fill(0);
    }

    Node(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<FileIndex, B>&& newPtrs)
        : BPNode(dir, idx, newKeyCount, std::move(newKeys))
        , ptrs(newPtrs)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot) override;
    virtual std::string Get(Key key) const override;

public:
    std::array<FileIndex, B> ptrs;
};

} // kv_storage

#endif // NODE_H
