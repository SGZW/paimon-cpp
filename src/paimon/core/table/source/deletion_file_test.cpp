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

#include "paimon/core/table/source/deletion_file.h"

#include <memory>

#include "gtest/gtest.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(DeletionFileTest, TestSimple) {
    {
        DeletionFile df("my_path", 100, 233, std::nullopt);
        ASSERT_EQ("{path = my_path, offset = 100, length = 233, cardinality = null}",
                  df.ToString());
    }
    {
        DeletionFile df("my_path", 100, 233, 234);
        ASSERT_EQ("{path = my_path, offset = 100, length = 233, cardinality = 234}", df.ToString());
    }
}

TEST(DeletionFileTest, TestSerializeAndDeserialize) {
    auto pool = GetDefaultPool();
    MemorySegmentOutputStream out(/*segment_size=*/8, pool);
    DeletionFile df("my_path", 100, 233, 234);
    ASSERT_EQ(df, df);
    DeletionFile::SerializeList({df, std::nullopt}, &out);
    auto bytes = MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    auto byte_array_input_stream =
        std::make_shared<ByteArrayInputStream>(bytes->data(), bytes->size());
    DataInputStream in(byte_array_input_stream);
    ASSERT_OK_AND_ASSIGN(std::vector<std::optional<DeletionFile>> deletion_files,
                         DeletionFile::DeserializeList(&in, /*version=*/4));
    ASSERT_EQ(2, deletion_files.size());
    ASSERT_TRUE(deletion_files[0]);
    ASSERT_EQ(deletion_files[0], df);
    ASSERT_FALSE(deletion_files[1]);
    ASSERT_EQ("{path = my_path, offset = 100, length = 233, cardinality = 234}",
              deletion_files[0].value().ToString());
}

}  // namespace paimon::test
