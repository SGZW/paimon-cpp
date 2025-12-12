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

#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/index/index_file_meta_v2_deserializer.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/result.h"

struct ArrowArray;

namespace paimon {
class MemoryPool;

class IndexFileMetaV3Deserializer : public ObjectSerializer<std::shared_ptr<IndexFileMeta>> {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_({
            arrow::field("_INDEX_TYPE", arrow::utf8(), false),
            arrow::field("_FILE_NAME", arrow::utf8(), false),
            arrow::field("_FILE_SIZE", arrow::int64(), false),
            arrow::field("_ROW_COUNT", arrow::int64(), false),
            arrow::field("_DELETIONS_VECTORS_RANGES",
                         arrow::list(arrow::field("item", DeletionVectorMeta::DataType(), true)),
                         true),
            arrow::field("_EXTERNAL_PATH", arrow::utf8(), true),
        });
        return schema;
    }

    explicit IndexFileMetaV3Deserializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<std::shared_ptr<IndexFileMeta>>(DataType(), pool) {}

    Result<BinaryRow> ToRow(const std::shared_ptr<IndexFileMeta>& meta) const override {
        assert(false);
        return Status::Invalid("IndexFileMetaV3Deserializer to row is not valid");
    }

    Result<std::shared_ptr<IndexFileMeta>> FromRow(const InternalRow& row) const override {
        auto file_type = row.GetString(0);
        auto file_name = row.GetString(1);
        auto file_size = row.GetLong(2);
        auto row_count = row.GetLong(3);
        std::optional<LinkedHashMap<std::string, DeletionVectorMeta>> dv_ranges;
        if (!row.IsNullAt(4)) {
            dv_ranges = IndexFileMetaV2Deserializer::RowArrayDataToDvRanges(row.GetArray(4).get());
        }
        std::optional<std::string> external_path;
        if (!row.IsNullAt(5)) {
            external_path = row.GetString(5).ToString();
        }
        return std::make_shared<IndexFileMeta>(file_type.ToString(), file_name.ToString(),
                                               file_size, row_count, dv_ranges, external_path);
    }
};

}  // namespace paimon
