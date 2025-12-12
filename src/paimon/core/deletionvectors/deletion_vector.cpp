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

#include "paimon/core/deletionvectors/deletion_vector.h"

#include <cstddef>
#include <string>

#include "fmt/format.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/core/table/source/deletion_file.h"
#include "paimon/fs/file_system.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeletionVector::DeserializeFromBytes(const Bytes* bytes,
                                                                               MemoryPool* pool) {
    return BitmapDeletionVector::Deserialize(bytes->data(), bytes->size(), pool);
}

Result<PAIMON_UNIQUE_PTR<DeletionVector>> DeletionVector::Read(const FileSystem* file_system,
                                                               const DeletionFile& deletion_file,
                                                               MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> input,
                           file_system->Open(deletion_file.path));
    DataInputStream file_input_stream(input);
    PAIMON_RETURN_NOT_OK(file_input_stream.Seek(deletion_file.offset));
    PAIMON_ASSIGN_OR_RAISE(int32_t actual_length, file_input_stream.ReadValue<int32_t>());
    if (actual_length != deletion_file.length) {
        return Status::Invalid(
            fmt::format("Size not match, actual size: {}, expect size: {}, , file path: {}",
                        actual_length, deletion_file.length, deletion_file.path));
    }
    auto bytes = Bytes::AllocateBytes(deletion_file.length, pool);
    PAIMON_RETURN_NOT_OK(file_input_stream.ReadBytes(bytes.get()));
    return DeserializeFromBytes(bytes.get(), pool);
}

PAIMON_UNIQUE_PTR<DeletionVector> DeletionVector::FromPrimitiveArray(
    const std::vector<char>& is_deleted, MemoryPool* pool) {
    RoaringBitmap32 roaring;
    for (size_t i = 0; i < is_deleted.size(); i++) {
        if (is_deleted[i] == static_cast<char>(1)) {
            roaring.Add(i);
        }
    }
    return pool->AllocateUnique<BitmapDeletionVector>(roaring);
}

}  // namespace paimon
