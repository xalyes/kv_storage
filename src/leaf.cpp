#include <fstream>
#include <array>

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

    std::array<Offset, MaxKeys> offsets;
    in.read(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));

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
}

template<>
void Leaf<std::string>::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);
    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));

    std::array<Offset, MaxKeys> offsets;
    offsets.fill(0);

    auto offsetPos = out.tellp();
    offsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(offsets) + 1;
    out.write(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));

    uint32_t i = 0;
    for (uint32_t i = 0; i < m_values.size(); i++)
    {
        out.write(m_values[i].data(), m_values[i].size());
        if (i && i < MaxKeys)
            offsets[i] = offsets[i - 1] + m_values[i - 1].size();
    }

    out.write(reinterpret_cast<char*>(&(m_nextBatch)), sizeof(m_nextBatch));

    out.seekp(offsetPos);
    out.write(reinterpret_cast<char*>(&(offsets)), sizeof(offsets));
}

} // kv_storage
