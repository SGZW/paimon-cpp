/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/append/bucketed_append_compact_manager.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/result.h"

namespace paimon::test {

class BucketedAppendCompactManagerTest : public testing::Test {
 public:
    void SetUp() override {}

    std::vector<std::shared_ptr<DataFileMeta>> GenerateDataFileMeta() {
        std::vector<std::shared_ptr<DataFileMeta>> metas;
        metas.push_back(DataFileMeta::ForAppend("file1", 100, 100, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/1,
                                                /*max_sequence_number=*/10, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file2", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/5,
                                                /*max_sequence_number=*/15, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        metas.push_back(DataFileMeta::ForAppend("file3", 200, 200, SimpleStats::EmptyStats(),
                                                /*min_sequence_number=*/20,
                                                /*max_sequence_number=*/30, 0, FileSource::Append(),
                                                std::nullopt, std::nullopt, std::nullopt,
                                                std::nullopt)
                            .value());
        return metas;
    }

 private:
};

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithoutOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(false);
    EXPECT_TRUE(comparator(file1, file2));
    EXPECT_FALSE(comparator(file2, file1));
    EXPECT_TRUE(comparator(file1, file3));
    EXPECT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestFileComparatorWithOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    auto comparator = BucketedAppendCompactManager::FileComparator(true);
    EXPECT_TRUE(comparator(file1, file2));
    EXPECT_FALSE(comparator(file2, file1));
    EXPECT_TRUE(comparator(file1, file3));
    EXPECT_FALSE(comparator(file3, file1));
}

TEST_F(BucketedAppendCompactManagerTest, TestIsOverlap) {
    auto files = GenerateDataFileMeta();
    auto& file1 = files[0];
    auto& file2 = files[1];
    auto& file3 = files[2];

    EXPECT_TRUE(BucketedAppendCompactManager::IsOverlap(file1, file2));
    EXPECT_FALSE(BucketedAppendCompactManager::IsOverlap(file1, file3));
    EXPECT_FALSE(BucketedAppendCompactManager::IsOverlap(file2, file3));
}

}  // namespace paimon::test
