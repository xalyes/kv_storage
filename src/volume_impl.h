#ifndef KV_STORAGE_VOLUME_IMPL_H
#define KV_STORAGE_VOLUME_IMPL_H

#include <memory>
#include <filesystem>

#include <kv_storage/volume.h>

namespace fs = std::filesystem;

namespace kv_storage {

using FileIndex = uint64_t;

class BPNode;

class VolumeImpl : public Volume
{
public:
    VolumeImpl(const fs::path& directory);

    virtual void Put(const Key& key, const std::string& value);
    virtual std::string Get(const Key& key);
    virtual void Delete(const Key& key);
    virtual ~VolumeImpl() = default;

private:
    std::shared_ptr<BPNode> m_root;
    const fs::path m_dir;
    FileIndex m_nodesCount;
    boost::compute::detail::lru_cache<FileIndex, std::shared_ptr<BPNode>> m_cache;
};

} // kv_storage

#endif // KV_STORAGE_VOLUME_IMPL_H
