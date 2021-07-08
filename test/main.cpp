#define BOOST_TEST_MODULE kv_storage tests
#include <boost/test/included/unit_test.hpp>

#include <map>
#include <string>
#include <array>
#include <memory>
#include <iostream>
#include <filesystem>

constexpr size_t B = 100;
constexpr size_t MaxKeys = B - 1;
using Key = uint64_t;
using FileIndex = uint64_t;

namespace fs = std::filesystem;

class IBatch
{
public:
    virtual void Load(const std::string& filename) = 0;
    virtual void Flush(const std::string& filename) = 0;
};

template<class T, size_t N>
void InsertToArray(std::array<T, N>& arr, size_t pos, T val)
{
    if (pos > arr.size())
        throw std::runtime_error("Invalid position for insert to std array");

    if (pos != arr.max_size())
    {
        for (size_t i = arr.size() - 1; i > pos; i--)
        {
            std::swap(arr[i], arr[i - 1]);
        }
    }
    arr[pos] = val;
}

class Node : public IBatch
{
public:
    Node()
    {
        keys.fill(0);
        ptrs.fill(0);
    }

    virtual void Load(const std::string& filename);
    virtual void Flush(const std::string& filename);

public:
    uint32_t keyCount{ 0 };
    std::array<Key, MaxKeys> keys;
    std::array<FileIndex, B> ptrs;
};

using Offset = uint64_t;
class Leaf : public IBatch
{
public:
    Leaf()
    {
        keys.fill(0);
        valuesOffsets.fill(0);
    }

    Leaf(uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<Offset, MaxKeys>&& newOffsets, std::vector<std::string>&& newValues, FileIndex newNextBatch)
        : keyCount(newKeyCount)
        , keys(newKeys)
        , valuesOffsets(newOffsets)
        , values(newValues)
        , nextBatch(newNextBatch)
    {}

    virtual void Load(const std::string& filename);
    virtual void Flush(const std::string& filename);
    void Put(Key key, std::string val);
    std::unique_ptr<Leaf> Split(FileIndex nextBatch);
    std::string Get(Key key) const;
    uint32_t GetKeyCount() const;
    Key GetFirstKey() const;

private:
    uint32_t keyCount{ 0 };
    std::array<Key, MaxKeys> keys;
    std::array<Offset, MaxKeys> valuesOffsets;
    std::vector<std::string> values;
    FileIndex nextBatch{ 0 };
};

uint32_t Leaf::GetKeyCount() const
{
    return keyCount;
}

Key Leaf::GetFirstKey() const
{
    return keys[0];
}

void Leaf::Put(Key key, std::string val)
{
    if (keyCount == 0)
    {
        InsertToArray(keys, 0, key);

        values.push_back(val);
        InsertToArray(valuesOffsets, 0, sizeof(keyCount) + sizeof(keys) + sizeof(valuesOffsets) + 1);

        keyCount++;
    }
    else
    {
        for (size_t i = 0; i < keyCount; i++)
        {
            auto currentKey = keys[i];

            if (key < currentKey)
            {
                if (keyCount != MaxKeys)
                {
                    InsertToArray(keys, i, key);

                    values.insert(values.begin()+i, val);
                    if (i != 0)
                        InsertToArray(valuesOffsets, i, valuesOffsets[i - 1] + values[i - 1].size());
                    else
                        InsertToArray(valuesOffsets, 0, sizeof(keyCount) + sizeof(keys) + sizeof(valuesOffsets) + 1);

                    keyCount++;

                    for (size_t j = i + 1; j < keyCount; j++)
                    {
                        valuesOffsets[j] += val.size();
                    }
                }
                else
                {
                    throw std::runtime_error("Try to insert value to fullfilled Leaf");
                }
                return;
            }

            if (currentKey == key)
            {
                throw std::runtime_error("Couldn't insert exising key");
            }
        }

        size_t i = keyCount;
        keys[i] = key;

        values.push_back(val);
        valuesOffsets[i] = valuesOffsets[i - 1] + values[i - 1].size();

        keyCount++;
    }
}

std::unique_ptr<Leaf> Leaf::Split(FileIndex nextBatch)
{
    uint32_t copyCount = MaxKeys / 2;

    std::array<Key, MaxKeys> newKeys;
    newKeys.fill(0);
    std::array<Offset, MaxKeys> newOffsets;
    newOffsets.fill(0);

    std::vector<std::string> newValues;

    newValues.insert(newValues.end(), std::make_move_iterator(values.begin() + copyCount + 1),
        std::make_move_iterator(values.end()));
    values.erase(values.begin() + copyCount + 1, values.end());

    std::swap(keys[copyCount + 1], newKeys[0]);
    newOffsets[0] = sizeof(keyCount) + sizeof(keys) + sizeof(valuesOffsets) + 1;

    for (uint32_t i = copyCount + 2; i < MaxKeys; i++)
    {
        std::swap(keys[i], newKeys[i - copyCount - 1]);

        newOffsets[i - copyCount - 1] = newOffsets[i - copyCount - 2] + newValues[i - copyCount - 1].size();
    }
    
    keyCount -= copyCount;
    return std::make_unique<Leaf>(copyCount, std::move(newKeys), std::move(newOffsets), std::move(newValues), nextBatch);
}

std::string Leaf::Get(Key key) const
{
    for (size_t i = 0; i < keyCount; i++)
    {
        if (keys[i] == key)
        {
            return values[i];
        }
    }

    throw std::runtime_error("Failed to get unexisted value of key");
}

struct BPNode
{
    BPNode(std::unique_ptr<Node>&& newNode)
    {
        isLeaf = false;
        node = std::move(newNode);
        leaf.reset();
    }

    BPNode(const std::string& batchFilename)
    {
        std::ifstream in;
        in.open(batchFilename, std::ios::in | std::ios::binary | std::ios::ate);
        if (!in.good())
        {
            leaf = std::make_unique<Leaf>();
            isLeaf = true;
            filename = batchFilename;
            return;
        }

        in.exceptions(~std::ifstream::goodbit);

        in.seekg(0);
        char type;
        in.read(&type, 1);

        // TODO: reuse ifstream instead of closing
        if (type == '8')
        {
            in.close();
            node = std::make_unique<Node>();
            node->Load(batchFilename);
            isLeaf = false;
        }
        else if (type == '9')
        {
            in.close();
            leaf = std::make_unique<Leaf>();
            leaf->Load(batchFilename);
            isLeaf = true;
        }
        else
        {
            throw std::runtime_error("Invalid file format");
        }

        filename = batchFilename;
    }

    std::string filename;
    bool isLeaf;
    std::unique_ptr<Node> node;
    std::unique_ptr<Leaf> leaf;
};

void Node::Load(const std::string& filename)
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(filename, std::ios::in | std::ios::binary);

    // TODO: check first char is 11
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(keyCount)), sizeof(keyCount));
    in.read(reinterpret_cast<char*>(&(keys)), sizeof(keys));
    in.read(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

void Node::Flush(const std::string& filename)
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);

    out.write(reinterpret_cast<char*>(&(keyCount)), sizeof(keyCount));
    out.write(reinterpret_cast<char*>(&(keys)), sizeof(keys));
    out.write(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

void Leaf::Load(const std::string& filename)
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(filename, std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();

    // TODO: check first char is 22
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(keyCount)), sizeof(keyCount));
    in.read(reinterpret_cast<char*>(&(keys)), sizeof(keys));
    in.read(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (uint64_t i = 0; i < keyCount; i++)
    {
        auto offset = valuesOffsets[i];
        uint64_t endOffset;

        if (i == keyCount - 1)
            endOffset = filesize - sizeof(nextBatch);
        else
            endOffset = valuesOffsets[i + 1];

        auto size = endOffset - offset;

        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        values.emplace_back(buf.get());
    }

    in.read(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

void Leaf::Flush(const std::string& filename)
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(filename, std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);
    out.write(reinterpret_cast<char*>(&(keyCount)), sizeof(keyCount));
    out.write(reinterpret_cast<char*>(&(keys)), sizeof(keys));
    out.write(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (const auto& v : values)
    {
        out.write(v.data(), v.size());
    }

    out.write(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

class Volume
{
public:
    Volume(const fs::path& path)
        : m_dir(path)
    {
        fs::create_directories(path);
    }

    void Put(const Key& key, const std::string& value);
    void Delete(const Key& key);
    std::string Get(const Key& key);
    void Flush();
    void Load();

private:
    std::unique_ptr<BPNode> m_root;
    const fs::path m_dir;
};

void Volume::Flush()
{
    m_root->leaf->Flush(m_root->filename);
}

void Volume::Load()
{
    m_root = std::make_unique<BPNode>((m_dir / "batch_1.dat").string());
}

void Volume::Put(const Key& key, const std::string& value)
{
    if (!m_root)
    {
        m_root = std::make_unique<BPNode>((m_dir / "batch_1.dat").string());

        m_root->leaf->Put(key, value);
        m_root->leaf->Flush(m_root->filename);
        return;
    }

    BPNode& current = *m_root;
    while (!current.isLeaf)
    {
        bool found = false;
        // TODO: binary search may be?
        Node& node = *current.node;
        for (uint32_t i = 0; i < node.keyCount; i++)
        {
            if (key <= node.keys[i])
            {
                // TODO: Impl some cache for loaded batches
                current = BPNode("batch_" + std::to_string(node.ptrs[i]) + ".dat");
                found = true;
                break;
            }
        }

        if (!found)
        {
            current = BPNode("batch_" + std::to_string(node.ptrs[node.keyCount]) + ".dat");
        }
    }

    if (current.leaf->GetKeyCount() == MaxKeys)
    {
        auto newLeaf = current.leaf->Split(0);

        if (key < newLeaf->GetFirstKey())
            current.leaf->Put(key, value);
        else
            newLeaf->Put(key, value);

        if (current.filename == m_root->filename)
        {
            // splitting root
            auto newRoot = std::make_unique<Node>();
            newRoot->keyCount = 2;
            newRoot->keys[0] = current.leaf->GetFirstKey();
            newRoot->keys[1] = newLeaf->GetFirstKey();
            newRoot->ptrs[0] = 2;
            newRoot->ptrs[1] = 3;

            current.leaf->Flush((m_dir / "batch_2.dat").string());
            newLeaf->Flush((m_dir / "batch_3.dat").string());
            newRoot->Flush((m_dir / "batch_1.dat").string());
            m_root = std::make_unique<BPNode>(std::move(newRoot));
            return;
        }
    }
    else
    {
        current.leaf->Put(key, value);
        current.leaf->Flush(current.filename);
    }

}

void Volume::Delete(const Key& key)
{
    // TODO
}

std::string Volume::Get(const Key& key)
{
    BPNode& current = *m_root;
    while (!current.isLeaf)
    {
        bool found = false;
        // TODO: binary search may be?
        Node& node = *current.node;
        for (uint32_t i = 0; i < node.keyCount; i++)
        {
            if (key <= node.keys[i])
            {
                // TODO: Impl some cache for loaded batches
                current = BPNode("batch_" + std::to_string(node.ptrs[i]) + ".dat");
                found = true;
                break;
            }
        }

        if (!found)
        {
            current = BPNode("batch_" + std::to_string(node.ptrs[node.keyCount]) + ".dat");
        }
    }

    return current.leaf->Get(key);
}

BOOST_AUTO_TEST_CASE(BasicTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    {
        Volume s(volumeDir);
        s.Put(33, "ololo");
        s.Put(44, "ololo2");
        s.Put(30, "ololo322");
        s.Put(1, "ololo4222");

        BOOST_TEST(s.Get(33) == "ololo");
        BOOST_TEST(s.Get(44) == "ololo2");
        s.Flush();
    }
    
    Volume s(volumeDir);
    s.Load();

    BOOST_TEST(s.Get(33) == "ololo");
    BOOST_TEST(s.Get(44) == "ololo2");
}

BOOST_AUTO_TEST_CASE(FewBatchesTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    {
        Volume s(volumeDir);
        for (int i = 0; i < MaxKeys; i++)
            s.Put(i, "ololo");

        s.Put(101, "ololo2");
        
        s.Flush();

        for (int i = 0; i < MaxKeys; i++)
            BOOST_TEST(s.Get(i) == "ololo");
    }

    Volume s(volumeDir);
    s.Load();

    for (int i = 0; i < MaxKeys; i++)
        BOOST_TEST(s.Get(i) == "ololo");
}
