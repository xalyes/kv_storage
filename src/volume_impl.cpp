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

    m_root = std::make_unique<Node>(m_dir, 1, 1, std::move(keys), std::move(ptrs));
    m_root->Flush();
}

void VolumeImpl::Delete(const Key& key)
{
    // TODO
}

std::string VolumeImpl::Get(const Key& key)
{
    return m_root->Get(key);
}

VolumeImpl::VolumeImpl(const fs::path& directory)
    : m_dir(directory)
{
    if (!fs::exists(m_dir / "batch_1.dat"))
    {
        fs::create_directories(m_dir);
        m_root = CreateEmptyBPNode(m_dir, 1);
        m_nodesCount = 1;
    }
    else
    {
        m_root = CreateBPNode(m_dir, 1);

        const auto batchFileFormat = std::regex("batch_\\d+\\.dat");
        m_nodesCount = std::count_if(fs::directory_iterator(m_dir), fs::directory_iterator{},
            [&batchFileFormat](const fs::path& p)
            {
                return std::regex_match(p.filename().string(), batchFileFormat);
            }
        );
    }
}

std::unique_ptr<Volume> CreateVolume(const std::filesystem::path& volumeDirectory)
{
    return std::make_unique<VolumeImpl>(volumeDirectory);
}

} // kv_storage