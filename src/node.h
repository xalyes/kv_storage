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
        m_ptrs.fill(0);
    }

    Node(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<FileIndex, B>&& newPtrs)
        : BPNode(dir, idx, newKeyCount, std::move(newKeys))
        , m_ptrs(newPtrs)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& value, FileIndex& nodesCount) override;
    virtual std::string Get(Key key) const override;
    virtual DeleteResult Delete(Key key, std::optional<Sibling> leftSibling, std::optional<Sibling> rightSibling) override;
    virtual Key GetMinimum() const override;

public:
    std::array<FileIndex, B> m_ptrs;
};

} // kv_storage

#endif // NODE_H
