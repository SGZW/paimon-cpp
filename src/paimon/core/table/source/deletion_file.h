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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// Deletion file for data file, the first 4 bytes are length, should, the following is the bitmap
/// content.
///
/// The first 4 bytes are length, should equal to `length`.
/// Next 4 bytes are the magic number, should be equal to 1581511376.
/// The remaining content should be a RoaringBitmap.
/// Member path indicates the deletion vector index file name of the corresponding data file in
/// DataSplits.
struct DeletionFile {
    DeletionFile(const std::string& _path, int64_t _offset, int64_t _length,
                 const std::optional<int64_t>& _cardinality)
        : path(_path), offset(_offset), length(_length), cardinality(_cardinality) {}

    bool operator==(const DeletionFile& other) const {
        if (this == &other) {
            return true;
        }
        return path == other.path && offset == other.offset && length == other.length &&
               cardinality == other.cardinality;
    }

    std::string ToString() const {
        return fmt::format(
            "{{path = {}, offset = {}, length = {}, cardinality = {}}}", path, offset, length,
            cardinality == std::nullopt ? "null" : std::to_string(cardinality.value()));
    }

    static void Serialize(const std::optional<DeletionFile>& file, MemorySegmentOutputStream* out) {
        if (file == std::nullopt) {
            out->WriteValue<char>(0);
        } else {
            out->WriteValue<char>(1);
            out->WriteString(file.value().path);
            out->WriteValue<int64_t>(file.value().offset);
            out->WriteValue<int64_t>(file.value().length);
            if (file.value().cardinality == std::nullopt) {
                out->WriteValue<int64_t>(-1);
            } else {
                out->WriteValue<int64_t>(file.value().cardinality.value());
            }
        }
    }

    static void SerializeList(const std::vector<std::optional<DeletionFile>>& files,
                              MemorySegmentOutputStream* out) {
        if (files.empty()) {
            out->WriteValue<char>(0);
        } else {
            out->WriteValue<char>(1);
            out->WriteValue<int32_t>(files.size());
            for (const auto& file : files) {
                Serialize(file, out);
            }
        }
    }

    static Result<std::vector<std::optional<DeletionFile>>> DeserializeList(DataInputStream* in,
                                                                            int32_t version) {
        std::vector<std::optional<DeletionFile>> files;
        PAIMON_ASSIGN_OR_RAISE(char has_deletion_file, in->ReadValue<char>());
        if (has_deletion_file == static_cast<char>(1)) {
            PAIMON_ASSIGN_OR_RAISE(int32_t size, in->ReadValue<int32_t>());
            files.reserve(size);
            for (int32_t i = 0; i < size; i++) {
                std::optional<DeletionFile> file;
                if (version >= 4) {
                    PAIMON_ASSIGN_OR_RAISE(file, Deserialize(in));
                } else if (version >= 1 && version <= 3) {
                    PAIMON_ASSIGN_OR_RAISE(file, DeserializeV3(in));
                } else {
                    return Status::Invalid(
                        fmt::format("Unsupported deletion file version: {}", version));
                }
                files.emplace_back(std::move(file));
            }
        }
        return files;
    }

 private:
    static Result<std::optional<DeletionFile>> Deserialize(DataInputStream* in) {
        char has_deletion_file = 0;
        PAIMON_ASSIGN_OR_RAISE(has_deletion_file, in->ReadValue<char>());
        if (has_deletion_file == static_cast<char>(0)) {
            return std::optional<DeletionFile>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::string path, in->ReadString());
        PAIMON_ASSIGN_OR_RAISE(int64_t offset, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t length, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t cardinality, in->ReadValue<int64_t>());
        return std::optional<DeletionFile>(
            DeletionFile(path, offset, length,
                         cardinality == -1 ? std::nullopt : std::optional<int64_t>(cardinality)));
    }

    static Result<std::optional<DeletionFile>> DeserializeV3(DataInputStream* in) {
        PAIMON_ASSIGN_OR_RAISE(char has_deletion_file, in->ReadValue<char>());
        if (has_deletion_file == static_cast<char>(0)) {
            return std::optional<DeletionFile>();
        }
        PAIMON_ASSIGN_OR_RAISE(std::string path, in->ReadString());
        PAIMON_ASSIGN_OR_RAISE(int64_t offset, in->ReadValue<int64_t>());
        PAIMON_ASSIGN_OR_RAISE(int64_t length, in->ReadValue<int64_t>());
        return std::optional<DeletionFile>(DeletionFile(path, offset, length, std::nullopt));
    }

 public:
    std::string path = "";
    int64_t offset = -1;
    int64_t length = -1;
    // the number of deleted rows.
    std::optional<int64_t> cardinality;
};
}  // namespace paimon
