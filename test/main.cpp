#define BOOST_TEST_MODULE kv_storage tests
#include <boost/test/included/unit_test.hpp>

#include <map>
#include <string>
#include <array>
#include <memory>
#include <iostream>
#include <filesystem>
#include <optional>
#include <regex>
#include <limits>
#include <random>
#include <fstream>
#include <set>

constexpr size_t B = 100;
constexpr size_t MaxKeys = B - 1;
using Key = uint64_t;
using FileIndex = uint64_t;

namespace fs = std::filesystem;

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

template<class T, size_t N>
void RemoveFromArray(std::array<T, N>& arr, size_t pos)
{
    if (pos > arr.size())
        throw std::runtime_error("Invalid position for removing from std array");

    arr[pos] = 0;
    if (pos != arr.max_size() - 1)
    {
        for (size_t i = pos; i < arr.max_size() - 1; i++)
        {
            std::swap(arr[i], arr[i + 1]);
        }
    }
}

class CreatedBPNode;

class BPNode
{
public:
    BPNode(const fs::path& dir, FileIndex idx)
        : m_dir(dir)
        , m_index(idx)
    {
        m_keys.fill(0);
    }

    BPNode(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys)
        : m_dir(dir)
        , m_index(idx)
        , m_keyCount(newKeyCount)
        , m_keys(newKeys)
    {}

    virtual ~BPNode() = default;

    virtual void Load() = 0;
    virtual void Flush() = 0;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot) = 0;
    virtual std::string Get(Key key) const = 0;
    virtual uint32_t GetKeyCount() const;
    virtual Key GetFirstKey() const;
    virtual FileIndex GetIndex() const;

protected:
    const fs::path m_dir;
    FileIndex m_index{ 0 };
    uint32_t m_keyCount{ 0 };
    std::array<Key, MaxKeys> m_keys;
};

// ptr to new created BPNode & key to be inserted to parent node
class CreatedBPNode
{
public:
    std::unique_ptr<BPNode> node;
    Key key;
};

uint32_t BPNode::GetKeyCount() const
{
    return m_keyCount;
}

Key BPNode::GetFirstKey() const
{
    return m_keys[0];
}

FileIndex BPNode::GetIndex() const
{
    return m_index;
}

class Node : public BPNode
{
public:
    Node(const fs::path& dir, FileIndex idx)
        : BPNode(dir, idx)
    {
        ptrs.fill(0);
    }

    Node(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<FileIndex, B>&& newPtrs)
        : BPNode(dir, idx, newKeyCount, std::move(newKeys))
        , ptrs(newPtrs)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot) override;
    std::string Get(Key key) const override;

public:
    std::array<FileIndex, B> ptrs;
};

using Offset = uint64_t;
class Leaf : public BPNode
{
public:
    Leaf(const fs::path& dir, FileIndex idx)
        : BPNode(dir, idx)
    {
        valuesOffsets.fill(0);
    }

    Leaf(const fs::path& dir, FileIndex idx, uint32_t newKeyCount, std::array<Key, MaxKeys>&& newKeys, std::array<Offset, MaxKeys>&& newOffsets, std::vector<std::string>&& newValues, FileIndex newNextBatch)
        : BPNode(dir, idx, newKeyCount, std::move(newKeys))
        , valuesOffsets(newOffsets)
        , values(newValues)
        , nextBatch(newNextBatch)
    {}

    virtual void Load() override;
    virtual void Flush() override;
    virtual std::optional<CreatedBPNode> Put(Key key, const std::string& val, FileIndex& nodesCount, bool isRoot) override;
    CreatedBPNode SplitAndPut(Key key, const std::string& value, FileIndex nextBatch, FileIndex& nodesCount);
    std::string Get(Key key) const override;

private:
    std::array<Offset, MaxKeys> valuesOffsets;
    std::vector<std::string> values;
    FileIndex nextBatch{ 0 };
};

std::unique_ptr<BPNode> CreateEmptyBPNode(const fs::path& dir, FileIndex idx)
{
    return std::make_unique<Leaf>(dir, idx);
}

std::unique_ptr<BPNode> CreateBPNode(const fs::path& dir, FileIndex idx)
{
    std::ifstream in;
    in.exceptions(~std::ifstream::goodbit);
    in.open(fs::path(dir) / ("batch_" + std::to_string(idx) + ".dat"), std::ios::in | std::ios::binary);

    char type;
    in.read(&type, 1);

    // TODO: reuse ifstream instead of closing
    if (type == '8')
    {
        in.close();
        auto node = std::make_unique<Node>(dir, idx);
        node->Load();
        return node;
    }
    else if (type == '9')
    {
        in.close();
        auto leaf = std::make_unique<Leaf>(dir, idx);
        leaf->Load();
        return leaf;
    }
    else
    {
        throw std::runtime_error("Invalid file format");
    }
}

std::string Node::Get(Key key) const
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, ptrs[m_keyCount]);

    return foundChild->Get(key);
}

std::optional<CreatedBPNode> Node::Put(Key key, const std::string& value, FileIndex& nodesCount, bool isRoot)
{
    std::unique_ptr<BPNode> foundChild;
    uint32_t childPos = m_keyCount;

    // TODO: binary search may be?
    for (uint32_t i = 0; i < m_keyCount; i++)
    {
        if (key < m_keys[i])
        {
            // TODO: Impl some cache for loaded batches
            foundChild = CreateBPNode(m_dir, ptrs[i]);
            childPos = i;
            break;
        }
    }

    if (!foundChild)
        foundChild = CreateBPNode(m_dir, ptrs[m_keyCount]);

    auto newNode = foundChild->Put(key, value, nodesCount, false);
    if (!newNode)
    {
        return std::nullopt;
    }
    // Child splitted

    if (m_keyCount == MaxKeys)
    {
        // Must split this node
        uint32_t copyCount = MaxKeys / 2;

        std::array<Key, MaxKeys> newKeys;
        newKeys.fill(0);
        std::array<FileIndex, B> newPtrs;
        newPtrs.fill(0);

        uint32_t borderIndex = MaxKeys - copyCount;

        std::swap(m_keys[borderIndex], newKeys[0]);
        std::swap(ptrs[borderIndex + 1], newPtrs[0]);

        for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
        {
            std::swap(m_keys[i], newKeys[i - borderIndex]);
            std::swap(ptrs[i + 1], newPtrs[i - borderIndex]);
        }

        m_keyCount -= copyCount;

        const auto insert = [](uint32_t& count, std::array<Key, MaxKeys>& keys, std::array<FileIndex, B>& ptrs, Key newKey, FileIndex newIdx)
        {
            for (uint32_t i = 0; i < count; i++)
            {
                auto currentKey = keys[i];

                if (newKey >= currentKey)
                    continue;

                InsertToArray(keys, i, newKey);
                InsertToArray(ptrs, i, newIdx);
                count++;
                return;
            }

            InsertToArray(keys, count, newKey);
            InsertToArray(ptrs, count, newIdx);
            count++;
        };

        Key firstNewKey = newNode.value().key;
        if (key < newKeys[0])
        {
            bool found = false;
            // insert to this
            for (uint32_t i = 0; i < m_keyCount; i++)
            {
                auto currentKey = m_keys[i];

                if (firstNewKey >= currentKey)
                    continue;

                InsertToArray(m_keys, i, firstNewKey);
                InsertToArray(ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                found = true;
                break;
            }

            if (!found)
            {
                InsertToArray(m_keys, m_keyCount, firstNewKey);
                InsertToArray(ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
                m_keyCount++;
            }
        }
        else
        {
            // insert to new node
            insert(copyCount, newKeys, newPtrs, firstNewKey, newNode.value().node->GetIndex());
        }

        auto keyToDelete = newKeys[0];
        RemoveFromArray(newKeys, 0);
        copyCount--;

        auto newNode = std::make_unique<Node>(m_dir, ++nodesCount, copyCount, std::move(newKeys), std::move(newPtrs));

        if (isRoot)
            m_index = ++nodesCount;

        newNode->Flush();
        Flush();

        return std::optional<CreatedBPNode>({ std::move(newNode), keyToDelete });
    }
    else
    {
        Key keyForInsert = newNode.value().key;

        for (uint32_t i = 0; i < m_keyCount; i++)
        {
            if (keyForInsert < m_keys[i])
            {
                InsertToArray(m_keys, i, keyForInsert);
                InsertToArray(ptrs, i + 1, newNode.value().node->GetIndex());
                m_keyCount++;
                Flush();
                return std::nullopt;
            }
        }

        InsertToArray(m_keys, m_keyCount, keyForInsert);
        InsertToArray(ptrs, m_keyCount + 1, newNode.value().node->GetIndex());
        m_keyCount++;

        Flush();
        return std::nullopt;
    }
}

std::optional<CreatedBPNode> Leaf::Put(Key key, const std::string& val, FileIndex& nodesCount, bool isRoot)
{
    if (m_keyCount == MaxKeys)
    {
        if (isRoot)
            m_index = ++nodesCount;

        return SplitAndPut(key, val, nextBatch, nodesCount);
    }

    if (m_keyCount == 0)
    {
        InsertToArray(m_keys, 0, key);

        values.push_back(val);
        InsertToArray(valuesOffsets, 0, sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1);

        m_keyCount++;
    }
    else
    {
        for (size_t i = 0; i < m_keyCount; i++)
        {
            auto currentKey = m_keys[i];

            if (key < currentKey)
            {
                if (m_keyCount != MaxKeys)
                {
                    InsertToArray(m_keys, i, key);

                    values.insert(values.begin()+i, val);
                    if (i != 0)
                        InsertToArray(valuesOffsets, i, valuesOffsets[i - 1] + values[i - 1].size());
                    else
                        InsertToArray(valuesOffsets, 0, sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1);

                    m_keyCount++;

                    for (size_t j = i + 1; j < m_keyCount; j++)
                    {
                        valuesOffsets[j] += val.size();
                    }
                }
                else
                {
                    throw std::runtime_error("Try to insert value to fullfilled Leaf");
                }
                Flush();
                return std::nullopt;
            }

            if (currentKey == key)
            {
                throw std::runtime_error("Couldn't insert exising key");
            }
        }

        size_t i = m_keyCount;
        m_keys[i] = key;

        values.push_back(val);
        valuesOffsets[i] = valuesOffsets[i - 1] + values[i - 1].size();

        m_keyCount++;
    }
    Flush();
    return std::nullopt;
}

CreatedBPNode Leaf::SplitAndPut(Key key, const std::string& value, FileIndex nextBatch, FileIndex& nodesCount)
{
    uint32_t copyCount = MaxKeys / 2;

    std::array<Key, MaxKeys> newKeys;
    newKeys.fill(0);
    std::array<Offset, MaxKeys> newOffsets;
    newOffsets.fill(0);

    std::vector<std::string> newValues;

    uint32_t borderIndex = values.size() - copyCount;

    newValues.insert(newValues.end(), std::make_move_iterator(values.begin() + borderIndex),
        std::make_move_iterator(values.end()));
    values.erase(values.begin() + borderIndex, values.end());

    std::swap(m_keys[borderIndex], newKeys[0]);
    newOffsets[0] = sizeof(m_keyCount) + sizeof(m_keys) + sizeof(valuesOffsets) + 1;

    for (uint32_t i = borderIndex + 1; i < MaxKeys; i++)
    {
        std::swap(m_keys[i], newKeys[i - borderIndex]);

        newOffsets[i - borderIndex] = newOffsets[i - borderIndex - 1] + newValues[i - borderIndex - 1].size();
    }
    
    m_keyCount -= copyCount;

    Key firstNewKey = newKeys[0];

    auto newLeaf = std::make_unique<Leaf>(m_dir, ++nodesCount, copyCount, std::move(newKeys), std::move(newOffsets), std::move(newValues), nextBatch);

    if (key < firstNewKey)
    {
        Put(key, value, nodesCount, false);
        newLeaf->Flush();
    }
    else
    {
        Flush();
        newLeaf->Put(key, value, nodesCount, false);
    }

    return { std::move(newLeaf), firstNewKey };
}

std::string Leaf::Get(Key key) const
{
    for (size_t i = 0; i < m_keyCount; i++)
    {
        if (m_keys[i] == key)
        {
            return values[i];
        }
    }

    throw std::runtime_error("Failed to get unexisted value of key '" + std::to_string(key) + "'");
}

void Node::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary);

    // TODO: check first char is 11
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    in.read(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

void Node::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("8", 1);

    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    out.write(reinterpret_cast<char*>(&(ptrs)), sizeof(ptrs));
}

void Leaf::Load()
{
    std::ifstream in;
    in.exceptions(~std::ofstream::goodbit);
    in.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::in | std::ios::binary | std::ios::ate);

    uint64_t filesize = in.tellg();

    // TODO: check first char is 22
    in.seekg(1);

    in.read(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    in.read(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    in.read(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (uint64_t i = 0; i < m_keyCount; i++)
    {
        auto offset = valuesOffsets[i];
        uint64_t endOffset;

        if (i == m_keyCount - 1)
            endOffset = filesize - sizeof(nextBatch);
        else
            endOffset = valuesOffsets[i + 1];

        auto size = endOffset - offset;

        auto buf = std::make_unique<char[]>(size + 1);
        in.read(buf.get(), size);
        buf.get()[size] = '\0';
        values.emplace_back(buf.get());
        //std::cout << "Reading " << buf << std::endl;
    }

    in.read(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

void Leaf::Flush()
{
    std::ofstream out;
    out.exceptions(~std::ofstream::goodbit);
    out.open(m_dir / ("batch_" + std::to_string(m_index) + ".dat"), std::ios::out | std::ios::binary | std::ios::trunc);

    out.write("9", 1);
    out.write(reinterpret_cast<char*>(&(m_keyCount)), sizeof(m_keyCount));
    out.write(reinterpret_cast<char*>(&(m_keys)), sizeof(m_keys));
    out.write(reinterpret_cast<char*>(&(valuesOffsets)), sizeof(valuesOffsets));

    for (const auto& v : values)
    {
        out.write(v.data(), v.size());
        //std::cout << "Writing " << v << std::endl;
    }

    out.write(reinterpret_cast<char*>(&(nextBatch)), sizeof(nextBatch));
}

class Volume
{
public:
    Volume(const fs::path& path)
        : m_dir(path)
    {
        if (!fs::exists(m_dir / "batch_1.dat"))
        {
            fs::create_directories(path);
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

    void Put(const Key& key, const std::string& value);
    void Delete(const Key& key);
    std::string Get(const Key& key);

private:
    std::unique_ptr<BPNode> m_root;
    const fs::path m_dir;
    FileIndex m_nodesCount;
};

void Volume::Put(const Key& key, const std::string& value)
{
    auto newNode = m_root->Put(key, value, m_nodesCount, true);
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

void Volume::Delete(const Key& key)
{
    // TODO
}

std::string Volume::Get(const Key& key)
{
    return m_root->Get(key);
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
    }
    
    Volume s(volumeDir);

    BOOST_TEST(s.Get(33) == "ololo");
    BOOST_TEST(s.Get(44) == "ololo2");
}

BOOST_AUTO_TEST_CASE(FewBatchesTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    std::set<std::string> keys;

    {
        Volume s(volumeDir);

        std::random_device rd;
        const uint32_t seed = rd();
        std::cout << "seed: " << seed << std::endl;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<uint64_t> uni(1, 25);

        for (int i = 0; i < 10000; i++)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s.Put(i, key);
        }

        for (int i = 19999; i >= 10000; i--)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s.Put(i, key);
        }

        for (int i = 0; i < 20000; i++)
        {
            BOOST_TEST(keys.count(s.Get(i)) == 1);
        }
    }

    Volume s(volumeDir);

    for (int i = 0; i < 20000; i++)
    {
        BOOST_TEST(keys.count(s.Get(i)) == 1);
    }
}
