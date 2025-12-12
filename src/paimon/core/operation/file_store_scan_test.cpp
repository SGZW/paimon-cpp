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

#include "paimon/core/operation/file_store_scan.h"

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class FileStoreScanTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

 private:
    std::shared_ptr<arrow::Schema> schema_ = arrow::schema(arrow::FieldVector(
        {arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
         arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())}));
};

TEST_F(FileStoreScanTest, TestCreatePartitionPredicate) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "Equal(f1, 1)");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionKeys) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = "hello";
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([Equal(f0, hello), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithDefaultPartitionName) {
    std::vector<std::string> partition_keys = {"f1", "f0"};

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    partition_filters["f0"] = partition_default_name;
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, {partition_filters}));
    ASSERT_EQ(partition_predicate->ToString(), "And([IsNull(f0), Equal(f1, 1)])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithMultiPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {{"f1", "2"}, {"f0", "world"}}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_EQ(partition_predicate->ToString(),
              "Or([And([Equal(f0, hello), Equal(f1, 1)]), And([Equal(f0, world), Equal(f1, 2)])])");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithOneEmptyPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    // empty map in partition_filters indicates a all_true partition filter
    std::vector<std::map<std::string, std::string>> partition_filters = {
        {{"f1", "1"}, {"f0", "hello"}}, {}};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithEmptyPartitionFilters) {
    std::vector<std::string> partition_keys = {"f1", "f0"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::vector<std::map<std::string, std::string>> partition_filters = {};
    ASSERT_OK_AND_ASSIGN(auto partition_predicate,
                         FileStoreScan::CreatePartitionPredicate(
                             partition_keys, partition_default_name, schema_, partition_filters));
    ASSERT_FALSE(partition_predicate);
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionKeys) {
    std::vector<std::string> partition_keys = {"invalid"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["f1"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in schema");
}

TEST_F(FileStoreScanTest, TestCreatePartitionPredicateWithInvalidPartitionFilter) {
    std::vector<std::string> partition_keys = {"f1"};
    std::string partition_default_name = "__DEFAULT_PARTITION__";
    std::map<std::string, std::string> partition_filters;
    partition_filters["invalid"] = "1";
    ASSERT_NOK_WITH_MSG(FileStoreScan::CreatePartitionPredicate(
                            partition_keys, partition_default_name, schema_, {partition_filters}),
                        "field invalid does not exist in partition keys");
}

}  // namespace paimon::test
