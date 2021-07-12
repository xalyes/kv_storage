#ifndef BP_NODE_H
#define BP_NODE_H

#include <array>
#include <string>
#include <memory>
#include <optional>
#include <filesystem>

namespace fs = std::filesystem;

namespace kv_storage {

constexpr size_t B = 100;
constexpr size_t MaxKeys = B - 1;
using Key = uint64_t;
using FileIndex = uint64_t;

class CreatedBPNode;

class BPNode
{
public:
    BPNode(const fs::path& dir, FileIndex idx)
        : m_dir(dir)
        , m_index(idx)
    {
        m_keys.fill(0);
    }

    BPNode(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys)
        : m_dir(dir)
        , m_index(idx)
        , m_keyCount(newKeyCount)
        , m_keys(newKeys)
    {}

    virtual ~BPNode() = default;

    virtual void Load() = 0;
    virtual void Flush() = 0;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot) = 0;
    virtual std::string Get(Key key) const = 0;
    virtual uint32_t GetKeyCount() const;
    virtual Key GetFirstKey() const;
    virtual FileIndex GetIndex() const;

protected:
    const fs::path m_dir;
    FileIndex m_index{ 0 };
    uint32_t m_keyCount{ 0 };
    std::array<Key, MaxKeys> m_keys;
};

// ptr to new created BPNode & key to be inserted to parent node
class CreatedBPNode
{
public:
    std::unique_ptr<BPNode> node;
    Key key;
};

std::unique_ptr<BPNode> CreateEmptyBPNode(const fs::path& dir, FileIndex idx);
std::unique_ptr<BPNode> CreateBPNode(const fs::path& dir, FileIndex idx);

} // kv_storage

#endif // BP_NODE_H
