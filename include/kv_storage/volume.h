#ifndef KV_STORAGE_VOLUME_H
#define KV_STORAGE_VOLUME_H

#include <string>
#include <memory>
#include <filesystem>

namespace kv_storage {

using Key = uint64_t;

class Volume
{
public:
    virtual void Put(const Key& key, const std::string& value) = 0;
    virtual std::string Get(const Key& key) = 0;
    virtual void Delete(const Key& key) = 0;
    virtual ~Volume() {};
};

std::unique_ptr<Volume> CreateVolume(const std::filesystem::path& volumeDirectory);

} // kv_storage

#endif // KV_STORAGE_VOLUME_H
