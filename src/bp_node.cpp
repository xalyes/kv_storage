#include "bp_node.h"
#include "leaf.h"
#include "node.h"

#include <fstream>

namespace kv_storage {

uint32_t BPNode::GetKeyCount() const
{
    return m_keyCount;
}

Key BPNode::GetLastKey() const
{
    return m_keys[m_keyCount - 1];
}

FileIndex BPNode::GetIndex() const
{
    return m_index;
}

void BPNode::SetIndex(FileIndex index)
{
    m_index = index;
}

std::unique_ptr<BPNode> CreateEmptyBPNode(const fs::path& dir, FileIndex idx)
{
    return std::make_unique<Leaf>(dir, idx);
}

std::unique_ptr<BPNode> CreateBPNode(const fs::path& dir, FileIndex idx)
{
    std::ifstream in;
    in.exceptions(~std::ifstream::goodbit);
    in.open(fs::path(dir) / ("batch_" + std::to_string(idx) + ".dat"), std::ios::in | std::ios::binary);

    char type;
    in.read(&type, 1);

    // TODO: reuse ifstream instead of closing
    if (type == '8')
    {
        in.close();
        auto node = std::make_unique<Node>(dir, idx);
        node->Load();
        return node;
    }
    else if (type == '9')
    {
        in.close();
        auto leaf = std::make_unique<Leaf>(dir, idx);
        leaf->Load();
        return leaf;
    }
    else
    {
        throw std::runtime_error("Invalid file format");
    }
}

} // kv_storage
