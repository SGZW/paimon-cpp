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
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "fmt/core.h"
#include "fmt/format.h"

namespace paimon {
/// Indicates the deletion vector info of member data_file_name, e.g., the length of dv.
/// * DeletionVectorMeta is used when serialize to manifest file.
struct DeletionVectorMeta {
    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_(
            {arrow::field("f0", arrow::utf8(), false), arrow::field("f1", arrow::int32(), false),
             arrow::field("f2", arrow::int32(), false),
             arrow::field("_CARDINALITY", arrow::int64(), true)});
        return schema;
    }
    DeletionVectorMeta(const std::string& _data_file_name, int32_t _offset, int32_t _length,
                       const std::optional<int64_t>& _cardinality)
        : data_file_name(_data_file_name),
          offset(_offset),
          length(_length),
          cardinality(_cardinality) {}

    bool operator==(const DeletionVectorMeta& other) const {
        if (this == &other) {
            return true;
        }
        return data_file_name == other.data_file_name && offset == other.offset &&
               length == other.length && cardinality == other.cardinality;
    }

    bool TEST_Equal(const DeletionVectorMeta& other) const {
        if (this == &other) {
            return true;
        }
        // ignore data_file_name
        return offset == other.offset && length == other.length && cardinality == other.cardinality;
    }

    std::string ToString() const {
        return fmt::format(
            "DeletionVectorMeta{{data_file_name = {}, offset = {}, length = {}, cardinality = {}}}",
            data_file_name, offset, length,
            cardinality == std::nullopt ? "null" : std::to_string(cardinality.value()));
    }

    std::string data_file_name = "";
    int32_t offset = -1;
    int32_t length = -1;
    std::optional<int64_t> cardinality;
};
}  // namespace paimon
