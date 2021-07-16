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
        auto s = kv_storage::CreateVolume(volumeDir);
        s->Put(33, "ololo");
        s->Put(44, "ololo2");
        s->Put(30, "ololo322");
        s->Put(1, "ololo4222");

        BOOST_TEST(s->Get(33) == "ololo");
        BOOST_TEST(s->Get(44) == "ololo2");
    }
    
    auto s = kv_storage::CreateVolume(volumeDir);

    BOOST_TEST(s->Get(33) == "ololo");
    BOOST_TEST(s->Get(44) == "ololo2");
}

BOOST_AUTO_TEST_CASE(FewBatchesTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    std::set<std::string> keys;

    {
        auto s = kv_storage::CreateVolume(volumeDir);

        std::random_device rd;
        const uint32_t seed = rd();
        std::cout << "seed: " << seed << std::endl;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<uint64_t> uni(1, 25);

        for (int i = 0; i < 10000; i++)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s->Put(i, key);
        }

        for (int i = 19999; i >= 10000; i--)
        {
            auto key = std::string(uni(rng), 'a') + std::to_string(i);
            keys.insert(key);
            s->Put(i, key);
        }

        for (int i = 0; i < 20000; i++)
        {
            BOOST_TEST(keys.count(s->Get(i)) == 1);
        }
    }

    auto s = kv_storage::CreateVolume(volumeDir);

    for (int i = 0; i < 20000; i++)
    {
        BOOST_TEST(keys.count(s->Get(i)) == 1);
    }
}

BOOST_AUTO_TEST_CASE(DeleteTest)
{
    fs::path volumeDir("vol");
    fs::remove_all(volumeDir);

    std::vector<int> keys;

    {
        const int count = 1000;

        auto s = kv_storage::CreateVolume(volumeDir);

        for (int i = 0; i < count; i++)
        {
            s->Put(i+1, "value" + std::to_string(i+1));
            keys.push_back(i+1);
        }

        std::random_device rd;
        const uint32_t seed = rd();
        std::cout << "seed: " << seed << std::endl;
        std::mt19937 rng(seed);

        std::shuffle(keys.begin(), keys.end(), rng);

        for (int i = 0; i < count; i++)
        {
            std::cout << "Deleting " << keys[i] << std::endl;
            s->Delete(keys[i]);

            for (int j = i + 1; j < count; j++)
            {
                BOOST_TEST(s->Get(keys[j]) == "value" + std::to_string(keys[j]));
            }
        }
    }
}
