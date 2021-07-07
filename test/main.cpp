#define BOOST_TEST_MODULE kv_storage tests
#include <boost/test/included/unit_test.hpp>

#include <map>
#include <string>
#include <array>
#include <memory>
#include <iostream>

constexpr size_t B = 100;
constexpr size_t MaxKeys = B - 1;
using Key = uint64_t;
using FileIndex = uint64_t;

struct Node
{
    size_t keyCount;
    std::array<Key, MaxKeys> keys;
    std::array<FileIndex, B> ptrs;
};

using Offset = uint64_t;
struct Leaf
{
    Leaf()
    {
        keys.fill(0);
        valuesOffsets.fill(0);
    }

    uint32_t keyCount{ 0 };
    std::array<Key, MaxKeys> keys;
    std::array<Offset, MaxKeys> valuesOffsets;
    std::vector<std::string> values;
    FileIndex nextBatch{ 0 };
};

class Volume
{
public:
    Volume()
    {}

    void Put(const Key& key, const std::string& value);
    void Delete(const Key& key);
    std::string Get(const Key& key);
    void Flush();
    void Load(const std::string& filename);

private:
    Leaf m_leaf{};
};

void Volume::Put(const Key& key, const std::string& value)
{
    m_leaf.keys[m_leaf.keyCount] = key;
    m_leaf.values.push_back(value);

    if (!m_leaf.keyCount)
        m_leaf.valuesOffsets[0] = sizeof(m_leaf.keyCount) + sizeof(m_leaf.keys) + sizeof(m_leaf.valuesOffsets);
    else
        m_leaf.valuesOffsets[m_leaf.keyCount] = m_leaf.valuesOffsets[m_leaf.keyCount - 1] + m_leaf.values[m_leaf.keyCount - 1].size();

    m_leaf.keyCount++;
}

void Volume::Delete(const Key& key)
{
    // TODO
}

std::string Volume::Get(const Key& key)
{
    for (size_t i = 0; i < m_leaf.keys.size(); i++)
    {
        if (m_leaf.keys[i] == key)
            return m_leaf.values[i];
    }
    throw std::runtime_error("Failed to get unexisted value of key");
}

void Volume::Load(const std::string& filename)
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(filename, std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();
    in.seekg(0);

    in.read(reinterpret_cast<char*>(&(m_leaf.keyCount)), sizeof(m_leaf.keyCount));
    in.read(reinterpret_cast<char*>(&(m_leaf.keys)), sizeof(m_leaf.keys));
    in.read(reinterpret_cast<char*>(&(m_leaf.valuesOffsets)), sizeof(m_leaf.valuesOffsets));

    for (uint64_t i = 0; i < m_leaf.keyCount; i++)
    {
        auto offset = m_leaf.valuesOffsets[i];
        uint64_t endOffset;

        if (i == m_leaf.keyCount - 1)
            endOffset = filesize - sizeof(m_leaf.nextBatch);
        else
            endOffset = m_leaf.valuesOffsets[i + 1];

        auto size = endOffset - offset;
        
        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        m_leaf.values.emplace_back(buf.get());
    }

    in.read(reinterpret_cast<char*>(&(m_leaf.nextBatch)), sizeof(m_leaf.nextBatch));
}

void Volume::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open("batch1.dat", std::ios::out | std::ios::binary | std::ios::trunc);

    out.write(reinterpret_cast<char*>(&(m_leaf.keyCount)), sizeof(m_leaf.keyCount));
    out.write(reinterpret_cast<char*>(&(m_leaf.keys)), sizeof(m_leaf.keys));
    out.write(reinterpret_cast<char*>(&(m_leaf.valuesOffsets)), sizeof(m_leaf.valuesOffsets));

    for (const auto& v : m_leaf.values)
    {
        out.write(v.data(), v.size());
    }

    out.write(reinterpret_cast<char*>(&(m_leaf.nextBatch)), sizeof(m_leaf.nextBatch));
}

BOOST_AUTO_TEST_CASE(BasicTests)
{
    {
        Volume s;
        s.Put(33, "ololo");
        s.Put(44, "ololo2");

        BOOST_TEST(s.Get(33) == "ololo");
        BOOST_TEST(s.Get(44) == "ololo2");
        s.Flush();
    }
    
    Volume s;
    s.Load("batch1.dat");

    BOOST_TEST(s.Get(33) == "ololo");
    BOOST_TEST(s.Get(44) == "ololo2");

}
