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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/utils/manifest_meta_reader.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

class AvroFileFormatTest : public testing::Test,
                           public ::testing::WithParamInterface<std::string> {};

INSTANTIATE_TEST_SUITE_P(Compression, AvroFileFormatTest,
                         ::testing::ValuesIn(std::vector<std::string>(
                             {"zstd", "zstandard", "snappy", "null", "deflate"})));

TEST_P(AvroFileFormatTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> file_format,
                         FileFormatFactory::Get("avro", {}));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};
    auto schema = arrow::schema(fields);
    auto data_type = arrow::struct_(fields);

    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(auto writer_builder, file_format->CreateWriterBuilder(&c_schema, 1024));
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    auto dir = ::paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<OutputStream> out,
        fs->Create(PathUtil::JoinPath(dir->Str(), "file.avro"), /*overwrite=*/false));

    auto compression = GetParam();
    ASSERT_OK_AND_ASSIGN(auto writer, writer_builder->Build(out, compression));

    std::string data_str =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    auto input_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, data_str).ValueOr(nullptr);
    ASSERT_TRUE(input_array);
    ::ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*input_array, &c_array).ok());
    ASSERT_OK(writer->AddBatch(&c_array));
    ASSERT_OK(writer->Flush());
    ASSERT_OK(writer->Finish());
    ASSERT_OK(out->Flush());
    ASSERT_OK(out->Close());

    // read
    ASSERT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(1024));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> in,
                         fs->Open(PathUtil::JoinPath(dir->Str(), "file.avro")));
    ASSERT_OK_AND_ASSIGN(auto batch_reader, reader_builder->Build(in));
    ASSERT_OK_AND_ASSIGN(auto chunked_array,
                         ::paimon::test::ReadResultCollector::CollectResult(batch_reader.get()));

    auto arrow_pool = GetArrowPool(GetDefaultPool());

    auto output_array = arrow::Concatenate(chunked_array->chunks()).ValueOr(nullptr);
    ASSERT_TRUE(output_array);
    ASSERT_OK_AND_ASSIGN(output_array, ManifestMetaReader::AlignArrayWithSchema(
                                           output_array, data_type, arrow_pool.get()));
    ASSERT_TRUE(output_array->Equals(input_array));
}

}  // namespace paimon::avro::test
