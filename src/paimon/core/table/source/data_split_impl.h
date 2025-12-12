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

#include "paimon/common/utils/object_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/io/data_file_meta_09_serializer.h"
#include "paimon/core/io/data_file_meta_10_serializer.h"
#include "paimon/core/io/data_file_meta_12_serializer.h"
#include "paimon/core/io/data_file_meta_first_row_id_legacy_serializer.h"
#include "paimon/core/io/data_file_meta_serializer.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/table/source/data_split.h"

namespace paimon {
/// Input splits. Needed by most batch computation engines.
class DataSplitImpl : public DataSplit {
 public:
    static constexpr int64_t MAGIC = -2394839472490812314L;
    static constexpr int32_t VERSION = 8;

    int64_t SnapshotId() const {
        return snapshot_id_;
    }

    const BinaryRow& Partition() const {
        return partition_;
    }

    int32_t Bucket() const {
        return bucket_;
    }

    const std::string& BucketPath() const {
        return bucket_path_;
    }

    const std::optional<int32_t>& TotalBuckets() const {
        return total_buckets_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& BeforeFiles() const {
        return before_files_;
    }

    const std::vector<std::optional<DeletionFile>>& BeforeDeletionFiles() const {
        return before_deletion_files_;
    }

    const std::vector<std::shared_ptr<DataFileMeta>>& DataFiles() const {
        return data_files_;
    }

    const std::vector<std::optional<DeletionFile>>& DeletionFiles() const {
        return data_deletion_files_;
    }

    bool IsStreaming() const {
        return is_streaming_;
    }

    bool RawConvertible() const {
        return raw_convertible_;
    }

    Result<std::optional<int64_t>> LatestFileCreationEpochMillis() const {
        if (data_files_.empty()) {
            return std::optional<int64_t>();
        }
        int64_t epoch = INT64_MIN;
        for (const auto& file : data_files_) {
            PAIMON_ASSIGN_OR_RAISE(int64_t epoch_milli, file->CreationTimeEpochMillis());
            epoch = std::max(epoch, epoch_milli);
        }
        return std::optional<int64_t>(epoch);
    }

    int64_t RowCount() const {
        int64_t row_count = 0;
        for (const auto& file : data_files_) {
            row_count += file->row_count;
        }
        return row_count;
    }

    std::vector<SimpleDataFileMeta> GetFileList() const override {
        std::vector<SimpleDataFileMeta> result_files;
        result_files.reserve(data_files_.size());
        for (const auto& file : data_files_) {
            std::string result_file_path;
            if (!file->external_path) {
                result_file_path = PathUtil::JoinPath(bucket_path_, file->file_name);
            } else {
                result_file_path = file->external_path.value();
            }
            result_files.push_back(SimpleDataFileMeta(
                {result_file_path, file->file_size, file->row_count, file->min_sequence_number,
                 file->max_sequence_number, file->schema_id, file->level, file->creation_time,
                 file->delete_row_count}));
        }
        return result_files;
    }

    bool operator==(const DataSplitImpl& other) const {
        if (this == &other) {
            return true;
        }
        return snapshot_id_ == other.snapshot_id_ && partition_ == other.partition_ &&
               bucket_ == other.bucket_ && bucket_path_ == other.bucket_path_ &&
               total_buckets_ == other.total_buckets_ &&
               ObjectUtils::Equal(before_files_, other.before_files_) &&
               before_deletion_files_ == other.before_deletion_files_ &&
               ObjectUtils::Equal(data_files_, other.data_files_) &&
               data_deletion_files_ == other.data_deletion_files_ &&
               is_streaming_ == other.is_streaming_ && raw_convertible_ == other.raw_convertible_;
    }

    bool TEST_Equal(const DataSplitImpl& other) const {
        if (this == &other) {
            return true;
        }
        return snapshot_id_ == other.snapshot_id_ && partition_ == other.partition_ &&
               bucket_ == other.bucket_ && bucket_path_ == other.bucket_path_ &&
               total_buckets_ == other.total_buckets_ &&
               ObjectUtils::TEST_Equal(before_files_, other.before_files_) &&
               before_deletion_files_ == other.before_deletion_files_ &&
               ObjectUtils::TEST_Equal(data_files_, other.data_files_) &&
               data_deletion_files_ == other.data_deletion_files_ &&
               is_streaming_ == other.is_streaming_ && raw_convertible_ == other.raw_convertible_;
    }

    /// Obtain merged row count as much as possible. There are two scenarios where accurate row
    /// count
    /// can be calculated:
    ///
    /// 1. raw file and no deletion file.
    ///
    /// 2. raw file + deletion file with cardinality.
    int64_t PartialMergedRowCount() const {
        if (!raw_convertible_) {
            return 0l;
        }
        int64_t sum = 0;
        for (size_t i = 0; i < data_files_.size(); i++) {
            const auto& data_file = data_files_[i];
            if (data_deletion_files_.empty() || data_deletion_files_[i] == std::nullopt) {
                sum += data_file->row_count;
            } else if (data_deletion_files_[i].value().cardinality != std::nullopt) {
                sum += data_file->row_count - data_deletion_files_[i].value().cardinality.value();
            }
        }
        return sum;
    }

    // Builder
    /// Builder for `DataSplitImpl`.
    class Builder {
     public:
        Builder(const BinaryRow& partition, int32_t bucket, const std::string& bucket_path,
                std::vector<std::shared_ptr<DataFileMeta>>&& data_files)
            : split_(std::shared_ptr<DataSplitImpl>(
                  new DataSplitImpl(partition, bucket, bucket_path, std::move(data_files)))) {}

        const std::vector<std::shared_ptr<DataFileMeta>>& DataFiles() {
            return split_->DataFiles();
        }

        Builder& WithTotalBuckets(const std::optional<int32_t>& total_buckets) {
            split_->total_buckets_ = total_buckets;
            return *this;
        }

        Builder& WithSnapshot(int64_t snapshot) {
            split_->snapshot_id_ = snapshot;
            return *this;
        }

        Builder& WithBeforeFiles(std::vector<std::shared_ptr<DataFileMeta>>&& before_files) {
            split_->before_files_ = std::move(before_files);
            return *this;
        }

        Builder& WithBeforeDeletionFiles(
            const std::vector<std::optional<DeletionFile>>& before_deletion_files) {
            split_->before_deletion_files_ = before_deletion_files;
            return *this;
        }

        Builder& WithDataDeletionFiles(
            const std::vector<std::optional<DeletionFile>>& data_deletion_files) {
            split_->data_deletion_files_ = data_deletion_files;
            return *this;
        }

        Builder& IsStreaming(bool is_streaming) {
            split_->is_streaming_ = is_streaming;
            return *this;
        }

        Builder& RawConvertible(bool raw_convertible) {
            split_->raw_convertible_ = raw_convertible;
            return *this;
        }

        Result<std::shared_ptr<DataSplitImpl>> Build() const {
            PAIMON_RETURN_NOT_OK(Preconditions::CheckArgument(split_->bucket_ != -1));
            return split_;
        }

     private:
        std::shared_ptr<DataSplitImpl> split_;
    };

    static Result<std::unique_ptr<ObjectSerializer<std::shared_ptr<DataFileMeta>>>>
    GetFileMetaSerializer(int32_t version, const std::shared_ptr<MemoryPool>& pool) {
        if (version == 1) {
            // TODO(xinyu.lxy): C++ paimon do not support data file meta 08
            return Status::NotImplemented("Do not support data file meta 08.");
        } else if (version == 2) {
            return std::make_unique<DataFileMeta09Serializer>(pool);
        } else if (version == 3 || version == 4) {
            return std::make_unique<DataFileMeta10Serializer>(pool);
        } else if (version == 5 || version == 6) {
            return std::make_unique<DataFileMeta12Serializer>(pool);
        } else if (version == 7) {
            return std::make_unique<DataFileMetaFirstRowIdLegacySerializer>(pool);
        } else if (version == VERSION) {
            return std::make_unique<DataFileMetaSerializer>(pool);
        } else {
            return Status::Invalid(fmt::format(
                "Expecting DataSplit version to be smaller or equal than {}, but found {}.",
                VERSION, version));
        }
    }

    std::string ToString() const {
        return fmt::format(
            "snapshotId={}, partition={}, bucket={}, bucketPath={}, totalBuckets={}, "
            "beforeFiles={}, "
            "beforeDeletionFiles={}, dataFiles={}, dataDeletionFiles={}, isStreaming={}, "
            "rawConvertible={}",
            snapshot_id_, partition_.ToString(), bucket_, bucket_path_,
            total_buckets_ == std::nullopt ? "null" : std::to_string(total_buckets_.value()),
            StringUtils::VectorToString(before_files_),
            StringUtils::VectorToString(before_deletion_files_),
            StringUtils::VectorToString(data_files_),
            StringUtils::VectorToString(data_deletion_files_), is_streaming_, raw_convertible_);
    }

 private:
    DataSplitImpl(const BinaryRow& partition, int32_t bucket, const std::string& bucket_path,
                  std::vector<std::shared_ptr<DataFileMeta>>&& data_files)
        : partition_(partition),
          bucket_(bucket),
          bucket_path_(bucket_path),
          data_files_(std::move(data_files)) {}

 private:
    int64_t snapshot_id_ = 0;
    BinaryRow partition_ = BinaryRow::EmptyRow();
    int32_t bucket_ = -1;
    std::string bucket_path_;
    std::optional<int32_t> total_buckets_;

    std::vector<std::shared_ptr<DataFileMeta>> before_files_;
    std::vector<std::optional<DeletionFile>> before_deletion_files_;
    std::vector<std::shared_ptr<DataFileMeta>> data_files_;
    std::vector<std::optional<DeletionFile>> data_deletion_files_;

    bool is_streaming_ = false;
    bool raw_convertible_ = false;
};
}  // namespace paimon
