#include <regex>

#include "bp_node.h"
#include "node.h"
#include "volume_impl.h"

namespace kv_storage {


void VolumeImpl::Put(const Key& key, const std::string& value)
{
    auto newNode = m_root->Put(key, value, m_nodesCount);
    if (!newNode)
        return;

    std::array<Key, MaxKeys> keys;
    keys.fill(0);
    std::array<FileIndex, B> ptrs;
    ptrs.fill(0);

    keys[0] = newNode.value().key;
    ptrs[0] = m_root->GetIndex();
    ptrs[1] = newNode.value().node->GetIndex();

    m_cache.insert(m_root->GetIndex(), m_root);

    m_root = std::make_unique<Node>(m_dir, m_cache, 1, 1, std::move(keys), std::move(ptrs));
    m_root->Flush();
    m_cache.insert(1, m_root);
}

void VolumeImpl::Delete(const Key& key)
{
    auto res = m_root->Delete(key, std::nullopt, std::nullopt);
    if (res.type == DeleteResult::Type::MergedRight || res.type == DeleteResult::Type::MergedLeft)
    {
        m_root = std::move(res.node);
        m_cache.insert(1, m_root);
        return;
    }
}

std::string VolumeImpl::Get(const Key& key)
{
    return m_root->Get(key);
}

VolumeImpl::VolumeImpl(const fs::path& directory)
    : m_dir(directory)
    , m_cache(100)
{
    if (!fs::exists(m_dir / "batch_1.dat"))
    {
        fs::create_directories(m_dir);
        m_root = CreateEmptyBPNode(m_dir, m_cache, 1);
    }
    else
    {
        m_root = CreateBPNode(m_dir, m_cache, 1);

        const auto batchFileFormat = std::regex("batch_\\d+\\.dat");
    }
    m_cache.insert(1, m_root);
    m_nodesCount = 1;
}

std::unique_ptr<Volume> CreateVolume(const std::filesystem::path& volumeDirectory)
{
    return std::make_unique<VolumeImpl>(volumeDirectory);
}

} // kv_storage