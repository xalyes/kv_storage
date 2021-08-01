#ifndef OUTDATED_KEYS_DELETER_H
#define OUTDATED_KEYS_DELETER_H

#include <fstream>

#include "leaf.h"

namespace fs = std::filesystem;

namespace kv_storage {

//-------------------------------------------------------------------------------
const std::chrono::duration AutoDeletePeriod = std::chrono::seconds(1);

//-------------------------------------------------------------------------------
template<class V>
class Volume;

//-------------------------------------------------------------------------------
//                            OutdatedKeysDeleter
//-------------------------------------------------------------------------------
template<class V>
class OutdatedKeysDeleter
{
public:
    OutdatedKeysDeleter(Volume<V>* volume, const fs::path& directory);
    ~OutdatedKeysDeleter();
    void Start();
    void Put(Key key, uint32_t ttl);
    void Delete(Key key);
    void Flush();
    void Load();

private:
    const fs::path m_dir;
    std::unordered_map<Key, uint64_t> m_ttls;
    std::thread m_worker;
    std::atomic_bool m_stop{ false };
    Volume<V>* m_volume;
};

//-------------------------------------------------------------------------------
template<class V>
OutdatedKeysDeleter<V>::OutdatedKeysDeleter(Volume<V>* volume, const fs::path& directory)
    : m_dir(directory)
{
    m_volume = volume;

    if (fs::exists(m_dir / "keys_ttls.dat"))
        Load();
}

//-------------------------------------------------------------------------------
template<class V>
OutdatedKeysDeleter<V>::~OutdatedKeysDeleter()
{
    if (m_worker.joinable())
    {
        m_stop = true;
        m_worker.join();
    }

    Flush();
}

//-------------------------------------------------------------------------------
template<class V>
void OutdatedKeysDeleter<V>::Start()
{
    if (m_worker.joinable())
        throw std::runtime_error("Worker thread already started");

    m_worker = std::thread{ [&]()
    {
        try
        {
            while (!m_stop)
            {
                const std::chrono::time_point now = std::chrono::system_clock::now();
                const std::chrono::system_clock::duration nowEpoch = now.time_since_epoch();
                const uint64_t nowSeconds = nowEpoch.count() * std::chrono::system_clock::period::num / std::chrono::system_clock::period::den;

                std::vector<Key> keysForDelete;

                for (const auto& item : m_ttls)
                {
                    if (m_stop)
                        break;

                    if (nowSeconds >= item.second)
                        keysForDelete.push_back(item.first);
                }

                for (const auto& key : keysForDelete)
                {
                    try
                    {
                        m_volume->Delete(key);
                    }
                    catch (const std::exception&)
                    {
                        // key not found
                    }
                    m_ttls.erase(key);
                }

                const auto elapsed = std::chrono::system_clock::now() - now;
                if (elapsed < AutoDeletePeriod)
                    std::this_thread::sleep_for(AutoDeletePeriod - elapsed);
            }
        }
        catch (const std::exception&)
        {
            // Fatal error
            return;
        }
    } };
}

//-------------------------------------------------------------------------------
template<class V>
void OutdatedKeysDeleter<V>::Put(Key key, uint32_t ttl)
{
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
    std::chrono::system_clock::duration dur = tp.time_since_epoch();

    uint64_t seconds = dur.count() * std::chrono::system_clock::period::num / std::chrono::system_clock::period::den;
    seconds += ttl;

    m_ttls.insert({ key, seconds });
}

//-------------------------------------------------------------------------------
template<class V>
void OutdatedKeysDeleter<V>::Delete(Key key)
{
    m_ttls.erase(key);
}

//-------------------------------------------------------------------------------
template<class V>
void OutdatedKeysDeleter<V>::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / "keys_ttls.dat", std::ios::out | std::ios::binary | std::ios::trunc);

    uint32_t count = boost::endian::native_to_little(m_ttls.size());
    out.write(reinterpret_cast<char*>(&count), sizeof(count));

    for (const auto& ttl : m_ttls)
    {
        auto key = boost::endian::native_to_little(ttl.first);
        out.write(reinterpret_cast<char*>(&key), sizeof(key));

        auto time = boost::endian::native_to_little(ttl.second);
        out.write(reinterpret_cast<char*>(&time), sizeof(time));
    }
}

//-------------------------------------------------------------------------------
template<class V>
void OutdatedKeysDeleter<V>::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / "keys_ttls.dat", std::ios::in | std::ios::binary);

    uint32_t count;
    in.read(reinterpret_cast<char*>(&(count)), sizeof(count));

    boost::endian::little_to_native_inplace(count);

    for (uint32_t i = 0; i < count; i++)
    {
        Key key;
        uint64_t time;

        in.read(reinterpret_cast<char*>(&(key)), sizeof(key));
        in.read(reinterpret_cast<char*>(&(time)), sizeof(time));

        boost::endian::little_to_native_inplace(key);
        boost::endian::little_to_native_inplace(time);

        m_ttls.insert({ key, time });
    }
}

} // kv_storage

#endif // OUTDATED_KEYS_DELETER_H
