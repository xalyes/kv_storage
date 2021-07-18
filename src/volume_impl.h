#ifndef KV_STORAGE_VOLUME_IMPL_H
#define KV_STORAGE_VOLUME_IMPL_H

#include <memory>
#include <filesystem>

#include <kv_storage/volume.h>

namespace fs = std::filesystem;

namespace kv_storage {

class VolumeEnumeratorImpl : public VolumeEnumerator
{
public:
    VolumeEnumeratorImpl(const fs::path& directory, BPCache& cache, std::shared_ptr<BPNode> firstBatch);
    virtual bool MoveNext();
    virtual std::pair<Key, std::string> GetCurrent();
    virtual ~VolumeEnumeratorImpl() = default;

private:
    std::shared_ptr<Leaf> m_currentBatch;
    int64_t m_counter{ -1 };
    const fs::path m_dir;
    BPCache& m_cache;
    bool isValid{ true };
};

using FileIndex = uint64_t;

class BPNode;

class VolumeImpl : public Volume
{
public:
    VolumeImpl(const fs::path& directory);

    virtual void Put(const Key& key, const std::string& value);
    virtual std::string Get(const Key& key);
    virtual void Delete(const Key& key);
    virtual std::unique_ptr<VolumeEnumerator> Enumerate();
    virtual ~VolumeImpl() = default;

private:
    std::shared_ptr<BPNode> m_root;
    const fs::path m_dir;
    FileIndex m_nodesCount;
    BPCache m_cache;
};

} // kv_storage

#endif // KV_STORAGE_VOLUME_IMPL_H
