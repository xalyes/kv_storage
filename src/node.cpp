#include <fstream>

#include <boost/endian/conversion.hpp>

#include <kv_storage/volume.h>

namespace kv_storage {

template<>
void Node<std::string>::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary);

    // TODO: check first char is "8"
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    in.read(reinterpret_cast<char*>(&(m_ptrs)), sizeof(m_ptrs));

    boost::endian::little_to_native_inplace(m_keyCount);

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        boost::endian::little_to_native_inplace(m_keys[i]);
        boost::endian::little_to_native_inplace(m_ptrs[i]);
    }
    boost::endian::little_to_native_inplace(m_ptrs[m_ptrs.size() - 1]);
}

template<>
void Node<std::string>::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("8", 1);

    auto keyCount = boost::endian::native_to_little(m_keyCount);
    out.write(reinterpret_cast<char*>(&keyCount), sizeof(keyCount));

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        auto key = boost::endian::native_to_little(m_keys[i]);
        out.write(reinterpret_cast<char*>(&key), sizeof(key));
    }

    for (uint32_t i = 0; i < m_ptrs.size(); i++)
    {
        auto ptr = boost::endian::native_to_little(m_ptrs[i]);
        out.write(reinterpret_cast<char*>(&ptr), sizeof(ptr));
    }
}

} // kv_storage
