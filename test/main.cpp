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

#include <kv_storage/volume.h>

namespace fs = std::filesystem;

BOOST_AUTO_TEST_CASE(BasicTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    {
        auto s = kv_storage::Volume<std::string>(volumeDir);
        s.Put(33, "ololo");
        s.Put(44, "ololo2");
        s.Put(30, "ololo322");
        s.Put(1, "ololo4222");

        BOOST_TEST(s.Get(33) == "ololo");
        BOOST_TEST(s.Get(44) == "ololo2");
    }
    
    auto s = kv_storage::Volume<std::string>(volumeDir);

    BOOST_TEST(s.Get(33) == "ololo");
    BOOST_TEST(s.Get(44) == "ololo2");
}

BOOST_AUTO_TEST_CASE(FewBatchesTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    std::set<std::string> keys;

    {
        auto s = kv_storage::Volume<std::string>(volumeDir);

        std::random_device rd;
        const uint32_t seed = rd();
        std::cout << "seed: " << seed << std::endl;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> uni(1, 25);

        for (int i = 0; i < 100000; i++)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s.Put(i, key);
        }

        for (int i = 199999; i >= 100000; i--)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s.Put(i, key);
        }

        for (int i = 0; i < 200000; i++)
        {
            BOOST_TEST(keys.count(s.Get(i)) == 1);
        }
    }

    auto s = kv_storage::Volume<std::string>(volumeDir);

    for (int i = 0; i < 200000; i++)
    {
        BOOST_TEST(keys.count(s.Get(i)) == 1);
    }
}

BOOST_AUTO_TEST_CASE(DeleteTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    std::vector<int> keys;

    {
        const int count = 40000;

        auto s = kv_storage::Volume<std::string>(volumeDir);

        for (int i = 0; i < count; i++)
        {
            s.Put(i+1, "value" + std::to_string(i+1));
            keys.push_back(i+1);
        }

        std::random_device rd;
        const uint32_t seed = rd();
        std::cout << "seed: " << seed << std::endl;
        std::mt19937 rng(seed);

        std::shuffle(keys.begin(), keys.end(), rng);

        for (int i = 0; i < count; i++)
        {
            //std::cout << "Deleting " << keys[i] << std::endl;
            s.Delete(keys[i]);

            if (i % 50 == 0)
            {
                for (int j = i + 1; j < count; j++)
                {
                    BOOST_TEST(s.Get(keys[j]) == "value" + std::to_string(keys[j]));
                }
            }
        }
    }
}

BOOST_AUTO_TEST_CASE(EnumeratorTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    auto s = kv_storage::Volume<std::string>(volumeDir);

    std::vector<int> keys;
    const int count = 10000;

    for (int i = 0; i < count; i++)
    {
        s.Put(i, "value" + std::to_string(i));
        keys.push_back(i);
    }

    {
        auto enumerator = s.Enumerate();

        for (auto k : keys)
        {
            BOOST_TEST(enumerator->MoveNext() == true);
            auto kv = enumerator->GetCurrent();
            BOOST_TEST(kv.first == k);
            BOOST_TEST(kv.second == "value" + std::to_string(k));
        }

        BOOST_TEST(enumerator->MoveNext() == false);
    }

    std::random_device rd;
    const uint32_t seed = rd();
    std::cout << "seed: " << seed << std::endl;
    std::mt19937 rng(seed);

    std::shuffle(keys.begin(), keys.end(), rng);

    for (int i = 0; i < count / 2; i++)
    {
        s.Delete(keys[i]);
    }

    keys.erase(keys.begin(), keys.begin() + count / 2);
    std::sort(keys.begin(), keys.end());

    auto enumerator = s.Enumerate();

    for (auto k : keys)
    {
        BOOST_TEST(enumerator->MoveNext() == true);
        auto kv = enumerator->GetCurrent();
        BOOST_TEST(kv.first == k);
        BOOST_TEST(kv.second == "value" + std::to_string(k));
    }

    BOOST_TEST(enumerator->MoveNext() == false);
}

BOOST_AUTO_TEST_CASE(MillionTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    const auto count = 1000000;
    std::string value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    {
        auto s = kv_storage::Volume<std::string>(volumeDir);

        {
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            for (int i = 0; i < count; i++)
            {
                s.Put(i, value);
            }

            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            std::cout << "Time elapsed for inserting values: " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
        }
    }

    {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        auto s = kv_storage::Volume<std::string>(volumeDir);

        for (int i = 0; i < count; i++)
        {
            BOOST_TEST(s.Get(i) == value);
        }

        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::cout << "Time elapsed for getting values: " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    }
}
