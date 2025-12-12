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

#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/result.h"

namespace paimon {
class MemoryPool;
class BinaryRowWriter;
/// An `ObjectSerializer` for `IndexFileMeta`.
class IndexFileMetaSerializer : public ObjectSerializer<std::shared_ptr<IndexFileMeta>> {
 public:
    explicit IndexFileMetaSerializer(const std::shared_ptr<MemoryPool>& pool)
        : ObjectSerializer<std::shared_ptr<IndexFileMeta>>(IndexFileMeta::DataType(), pool) {}

    Result<BinaryRow> ToRow(const std::shared_ptr<IndexFileMeta>& meta) const override;

    Result<std::shared_ptr<IndexFileMeta>> FromRow(const InternalRow& row) const override;

    static void WriteIndexFileMeta(int32_t start_pos, const std::shared_ptr<IndexFileMeta>& meta,
                                   BinaryRowWriter* writer, MemoryPool* pool);

    static BinaryArray DvRangesToRowArrayData(
        const LinkedHashMap<std::string, DeletionVectorMeta>& dv_metas, MemoryPool* pool);
};

}  // namespace paimon
