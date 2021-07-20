#include <fstream>
#include <array>
#include <boost/endian/conversion.hpp>

#include "leaf.h"

namespace kv_storage {

template<>
void Leaf<std::string>::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();

    // TODO: check first char is "9"
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));

    boost::endian::little_to_native_inplace(m_keyCount);

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        boost::endian::little_to_native_inplace(m_keys[i]);
    }

    std::array<Offset, MaxKeys> offsets;
    in.read(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));

    for (uint32_t i = 0; i < offsets.size(); i++)
    {
        boost::endian::little_to_native_inplace(offsets[i]);
    }

    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        auto offset = offsets[i];
        uint64_t endOffset;

        if (i == m_keyCount - 1)
            endOffset = filesize - sizeof(m_nextBatch);
        else
            endOffset = offsets[i + 1];

        auto size = endOffset - offset;

        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        m_values.emplace_back(buf.get());
    }

    in.read(reinterpret_cast<char*>(&(m_nextBatch)), sizeof(m_nextBatch));
    boost::endian::little_to_native_inplace(m_nextBatch);
}

template<>
void Leaf<std::string>::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);

    auto keyCount = boost::endian::native_to_little(m_keyCount);
    out.write(reinterpret_cast<char*>(&keyCount), sizeof(keyCount));

    for (uint32_t i = 0; i < m_keys.size(); i++)
    {
        auto key = boost::endian::native_to_little(m_keys[i]);
        out.write(reinterpret_cast<char*>(&key), sizeof(key));
    }

    std::array<Offset, MaxKeys> offsets;
    offsets.fill(0);

    auto offsetPos = out.tellp();
    offsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(offsets) + 1;
    out.write(reinterpret_cast<char*>(&offsets), sizeof(offsets));

    for (uint32_t i = 0; i < m_values.size(); i++)
    {
        out.write(m_values[i].data(), m_values[i].size());
        if (i && i < MaxKeys)
            offsets[i] = offsets[i - 1] + m_values[i - 1].size();
    }

    auto nextBatch = boost::endian::native_to_little(m_nextBatch);
    out.write(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));

    out.seekp(offsetPos);

    for (uint32_t i = 0; i < offsets.size(); i++)
    {
        auto o = boost::endian::native_to_little(offsets[i]);
        out.write(reinterpret_cast<char*>(&o), sizeof(o));
    }
}

} // kv_storage
