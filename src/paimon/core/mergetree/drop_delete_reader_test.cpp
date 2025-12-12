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

#include "paimon/core/mergetree/drop_delete_reader.h"

#include <cstddef>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
class Metrics;
}  // namespace paimon

namespace paimon::test {
class DropDeleteReaderTest : public testing::Test {
 public:
    class FakeSortMergeReader : public SortMergeReader {
     public:
        explicit FakeSortMergeReader(std::vector<KeyValue>&& data) : data_(std::move(data)) {}

        class Iterator : public SortMergeReader::Iterator {
         public:
            explicit Iterator(FakeSortMergeReader* reader) : reader_(reader) {}
            Result<bool> HasNext() override {
                return reader_->iter_ < reader_->data_.size();
            }
            KeyValue&& Next() override {
                return std::move(reader_->data_[reader_->iter_++]);
            }

         private:
            FakeSortMergeReader* reader_;
        };

        Result<std::unique_ptr<SortMergeReader::Iterator>> NextBatch() override {
            if (iter_ < data_.size()) {
                return std::make_unique<Iterator>(this);
            }
            return std::unique_ptr<SortMergeReader::Iterator>();
        }

        std::shared_ptr<Metrics> GetReaderMetrics() const override {
            return nullptr;
        }

        void Close() override {}

     private:
        std::vector<KeyValue> data_;
        size_t iter_ = 0;
    };
};

TEST_F(DropDeleteReaderTest, TestSimple) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::UpdateAfter(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Delete(), /*sequence_number=*/2, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({20}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({20, 200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/3, /*level=*/1, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({30}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({30, 300}, pool.get()));
    KeyValue kv4(RowKind::UpdateBefore(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({40}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({40, 100}, pool.get()));
    std::vector<KeyValue> kvs;
    kvs.reserve(4);
    kvs.push_back(std::move(kv1));
    kvs.push_back(std::move(kv2));
    kvs.push_back(std::move(kv3));
    kvs.push_back(std::move(kv4));

    auto sort_merge_reader = std::make_unique<FakeSortMergeReader>(std::move(kvs));
    auto drop_delete_reader = std::make_unique<DropDeleteReader>(std::move(sort_merge_reader));

    ASSERT_OK_AND_ASSIGN(
        std::vector<KeyValue> results,
        (ReadResultCollector::CollectKeyValueResult<SortMergeReader, SortMergeReader::Iterator>(
            drop_delete_reader.get())));
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0].value->GetInt(0), 10);
    ASSERT_EQ(results[0].value->GetInt(1), 100);
    ASSERT_EQ(results[1].value->GetInt(0), 30);
    ASSERT_EQ(results[1].value->GetInt(1), 300);
}
}  // namespace paimon::test
