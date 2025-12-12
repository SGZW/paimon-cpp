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

#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/c/bridge.h"
#include "arrow/result.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {

/// A rolling file writer that handles both normal data and blob data. This writer creates separate
/// files for normal columns and blob columns, managing their lifecycle and ensuring consistency
/// between them.
///
/// <pre>
/// For example,
/// given a table schema with normal columns (id INT, name STRING) and a blob column (data BLOB),
/// this writer will create separate files for (id, name) and (data).
/// It will roll files based on the specified target file size, ensuring that both normal and blob
/// files are rolled simultaneously.
///
/// Every time a file is rolled, the writer will close the current normal data file and blob data
/// files, so one normal data file may correspond to multiple blob data files.
///
/// Normal file1: f1.parquet may including (b1.blob, b2.blob, b3.blob)
/// Normal file2: f1-2.parquet may including (b4.blob, b5.blob)
///
/// </pre>
class RollingBlobFileWriter
    : public RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>> {
 public:
    using MainWriter = SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;
    using BlobWriter = RollingFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>;

    // Expected number of blob fields in a table.
    static constexpr int32_t EXPECTED_BLOB_FIELD_COUNT = 1;

    RollingBlobFileWriter(
        int64_t target_file_size,
        std::function<Result<std::unique_ptr<MainWriter>>()> create_file_writer,
        std::function<Result<std::unique_ptr<BlobWriter>>()> create_blob_file_writer,
        const std::shared_ptr<arrow::DataType>& data_type);
    ~RollingBlobFileWriter() override = default;

    Status Write(::ArrowArray* record) override;
    void Abort() override;
    Status Close() override;
    Result<std::vector<std::shared_ptr<DataFileMeta>>> GetResult() override;

 private:
    static Status ValidateFileConsistency(
        const std::shared_ptr<DataFileMeta>& main_data_file_meta,
        const std::vector<std::shared_ptr<DataFileMeta>>& blob_tagged_metas);

    Status CloseCurrentWriter();

    Result<std::shared_ptr<DataFileMeta>> CloseMainWriter();
    Result<std::vector<std::shared_ptr<DataFileMeta>>> CloseBlobWriter();

    std::function<Result<std::unique_ptr<BlobWriter>>()> create_blob_file_writer_;
    std::unique_ptr<BlobWriter> blob_writer_;
    std::shared_ptr<arrow::DataType> data_type_;

    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon
