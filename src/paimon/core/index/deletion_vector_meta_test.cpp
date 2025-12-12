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

#include "paimon/core/index/deletion_vector_meta.h"

#include "gtest/gtest.h"

namespace paimon::test {
TEST(DeletionVectorMetaTest, EqualityOperator) {
    DeletionVectorMeta meta1("file1", 0, 100, 1000);
    DeletionVectorMeta meta2("file1", 0, 100, 1000);
    DeletionVectorMeta meta3("file2", 0, 100, 1000);

    EXPECT_TRUE(meta1 == meta2);
    EXPECT_FALSE(meta1 == meta3);
}

TEST(DeletionVectorMetaTest, ToString) {
    DeletionVectorMeta meta("file1", 0, 100, 1000);
    std::string expected =
        "DeletionVectorMeta{data_file_name = file1, offset = 0, length = 100, cardinality = 1000}";
    EXPECT_EQ(meta.ToString(), expected);

    DeletionVectorMeta meta_null("file1", 0, 100, std::nullopt);
    expected =
        "DeletionVectorMeta{data_file_name = file1, offset = 0, length = 100, cardinality = null}";
    EXPECT_EQ(meta_null.ToString(), expected);
}

}  // namespace paimon::test
