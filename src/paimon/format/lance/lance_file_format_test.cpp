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

#include "paimon/format/lance/lance_file_format.h"

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/lance/lance_file_batch_reader.h"
#include "paimon/format/lance/lance_file_format_factory.h"
#include "paimon/format/lance/lance_format_defs.h"
#include "paimon/format/lance/lance_format_writer.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lance::test {
TEST(LanceFileFormatTest, TestSimple) {
    LanceFileFormatFactory factory;
    std::map<std::string, std::string> options = {{LANCE_READAHEAD_BATCH_COUNT, "2"}};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileFormat> lance_format, factory.Create(options));
    ASSERT_TRUE(lance_format);

    // create reader
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<ReaderBuilder> reader_builder,
                         lance_format->CreateReaderBuilder(/*batch_size=*/1000));
    ASSERT_TRUE(reader_builder);
    std::string path = paimon::test::GetDataDir() + "/lance/test.lance";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileBatchReader> file_reader, reader_builder->Build(path));
    ASSERT_TRUE(file_reader);
    auto lance_reader = dynamic_cast<LanceFileBatchReader*>(file_reader.get());
    ASSERT_TRUE(lance_reader);
    ASSERT_EQ(lance_reader->batch_size_, 1000);
    ASSERT_EQ(lance_reader->batch_readahead_, 2);

    // create writer
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::utf8())};
    auto schema = arrow::schema(fields);
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriterBuilder> writer_builder,
                         lance_format->CreateWriterBuilder(&c_schema, /*batch_size=*/1000));
    ASSERT_TRUE(writer_builder);

    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto direct_writer_builder = dynamic_cast<DirectWriterBuilder*>(writer_builder.get());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FormatWriter> writer,
                         direct_writer_builder->BuildFromPath(dir->Str() + "/test.lance"));
    ASSERT_TRUE(writer);
    auto lance_writer = dynamic_cast<LanceFormatWriter*>(writer.get());
    ASSERT_TRUE(lance_writer);
}
}  // namespace paimon::lance::test
