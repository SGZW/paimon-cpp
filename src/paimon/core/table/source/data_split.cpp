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

#include "paimon/table/source/data_split.h"

#include <utility>

#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/serialization_utils.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/core/table/source/fallback_data_split.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {
struct DataFileMeta;

Result<std::string> DataSplit::Serialize(const std::shared_ptr<DataSplit>& data_split,
                                         const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(data_split);
    if (!data_split_impl) {
        return Status::Invalid("invalid data split");
    }
    out.WriteValue<int64_t>(DataSplitImpl::MAGIC);
    out.WriteValue<int32_t>(DataSplitImpl::VERSION);
    out.WriteValue<int64_t>(data_split_impl->SnapshotId());

    PAIMON_RETURN_NOT_OK(
        SerializationUtils::SerializeBinaryRow(data_split_impl->Partition(), &out));
    out.WriteValue<int32_t>(data_split_impl->Bucket());
    out.WriteString(data_split_impl->BucketPath());

    std::optional<int32_t> total_buckets = data_split_impl->TotalBuckets();
    if (total_buckets == std::nullopt) {
        out.WriteValue<bool>(false);
    } else {
        out.WriteValue<bool>(true);
        out.WriteValue<int32_t>(total_buckets.value());
    }

    DataFileMetaSerializer serializer(pool);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->BeforeFiles(), &out));

    DeletionFile::SerializeList(data_split_impl->BeforeDeletionFiles(), &out);
    PAIMON_RETURN_NOT_OK(serializer.SerializeList(data_split_impl->DataFiles(), &out));
    DeletionFile::SerializeList(data_split_impl->DeletionFiles(), &out);
    out.WriteValue<bool>(data_split_impl->IsStreaming());
    out.WriteValue<bool>(data_split_impl->RawConvertible());

    PAIMON_UNIQUE_PTR<Bytes> bytes =
        MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
    return std::string(bytes->data(), bytes->size());
}

Result<std::shared_ptr<DataSplit>> DataSplit::Deserialize(const char* buffer, size_t length,
                                                          const std::shared_ptr<MemoryPool>& pool) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);
    int64_t magic = -1;
    PAIMON_ASSIGN_OR_RAISE(magic, in.ReadValue<int64_t>());
    int32_t version = 1;
    if (magic == DataSplitImpl::MAGIC) {
        PAIMON_ASSIGN_OR_RAISE(version, in.ReadValue<int32_t>());
    }

    // version 1 does not write magic number in, so the first long is snapshot id.
    int64_t snapshot_id = magic;
    if (version != 1) {
        PAIMON_ASSIGN_OR_RAISE(snapshot_id, in.ReadValue<int64_t>());
    }

    PAIMON_ASSIGN_OR_RAISE(BinaryRow partition,
                           SerializationUtils::DeserializeBinaryRow(&in, pool.get()));
    int32_t bucket = -1;
    PAIMON_ASSIGN_OR_RAISE(bucket, in.ReadValue<int32_t>());
    std::string bucket_path;
    PAIMON_ASSIGN_OR_RAISE(bucket_path, in.ReadString());

    std::optional<int32_t> total_buckets;
    if (version >= 6) {
        PAIMON_ASSIGN_OR_RAISE(bool total_buckets_exist, in.ReadValue<bool>());
        if (total_buckets_exist) {
            PAIMON_ASSIGN_OR_RAISE(total_buckets, in.ReadValue<int32_t>());
        }
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>> data_file_serializer,
        DataSplitImpl::GetFileMetaSerializer(version, pool));
    std::vector<std::shared_ptr<DataFileMeta>> before_files;
    PAIMON_ASSIGN_OR_RAISE(before_files, data_file_serializer->DeserializeList(&in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> before_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(before_deletion_files, DeletionFile::DeserializeList(&in, version));

    std::vector<std::shared_ptr<DataFileMeta>> data_files;
    PAIMON_ASSIGN_OR_RAISE(data_files, data_file_serializer->DeserializeList(&in));
    // compatible for deletion file
    std::vector<std::optional<DeletionFile>> data_deletion_files;
    PAIMON_ASSIGN_OR_RAISE(data_deletion_files, DeletionFile::DeserializeList(&in, version));

    bool is_streaming = false;
    PAIMON_ASSIGN_OR_RAISE(is_streaming, in.ReadValue<bool>());
    bool raw_convertible = false;
    PAIMON_ASSIGN_OR_RAISE(raw_convertible, in.ReadValue<bool>());

    DataSplitImpl::Builder builder(partition, bucket, bucket_path, std::move(data_files));
    builder.WithTotalBuckets(total_buckets)
        .WithSnapshot(snapshot_id)
        .WithBeforeFiles(std::move(before_files))
        .IsStreaming(is_streaming)
        .RawConvertible(raw_convertible);
    if (!before_deletion_files.empty()) {
        builder.WithBeforeDeletionFiles(before_deletion_files);
    }
    if (!data_deletion_files.empty()) {
        builder.WithDataDeletionFiles(data_deletion_files);
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataSplit> data_split, builder.Build());

    PAIMON_ASSIGN_OR_RAISE(int64_t pos, in.GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t stream_length, in.Length());
    if (pos == stream_length) {
        return data_split;
    } else if (pos == stream_length - 1) {
        PAIMON_ASSIGN_OR_RAISE(bool is_fallback, in.ReadValue<bool>());
        return std::make_shared<FallbackDataSplit>(data_split, is_fallback);
    } else {
        return Status::Invalid(
            fmt::format("invalid data split byte stream, remaining {} bytes after deserializing",
                        stream_length - pos));
    }
}

bool DataSplit::SimpleDataFileMeta::operator==(const DataSplit::SimpleDataFileMeta& other) const {
    if (this == &other) {
        return true;
    }
    return file_path == other.file_path && file_size == other.file_size &&
           row_count == other.row_count && min_sequence_number == other.min_sequence_number &&
           max_sequence_number == other.max_sequence_number && schema_id == other.schema_id &&
           level == other.level && creation_time == other.creation_time &&
           delete_row_count == other.delete_row_count;
}

std::string DataSplit::SimpleDataFileMeta::ToString() const {
    return fmt::format(
        "{{filePath: {}, fileSize: {}, rowCount: {}, minSequenceNumber: {}, maxSequenceNumber: {}, "
        "schemaId: {}, level: {}, creationTime: {}, deleteRowCount: {}}}",
        file_path, file_size, row_count, min_sequence_number, max_sequence_number, schema_id, level,
        creation_time.ToString(),
        delete_row_count == std::nullopt ? "null" : std::to_string(delete_row_count.value()));
}

}  // namespace paimon
