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
#include <string>
#include <string_view>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_binary.h"
#include "avro/GenericDatum.hh"
#include "avro/ValidSchema.hh"
#include "paimon/result.h"

namespace arrow {
class Array;
class DataType;
}  // namespace arrow
namespace avro {
class ValidSchema;
}  // namespace avro

namespace paimon::avro {

class AvroAdaptor {
 public:
    explicit AvroAdaptor(const std::shared_ptr<arrow::DataType>& type) : type_(type) {}

    Result<std::vector<::avro::GenericDatum>> ConvertArrayToGenericDatums(
        const std::shared_ptr<arrow::Array>& array, const ::avro::ValidSchema& avro_schema) const;

 private:
    Result<::avro::GenericDatum> ConvertArrayToGenericDatum(
        const ::avro::ValidSchema& avro_schema, const std::shared_ptr<arrow::Array>& arrow_array,
        int32_t row_idx) const;

    template <typename T, typename A>
    static void SetValue(int32_t row_idx, const std::shared_ptr<A>& array,
                         ::avro::GenericDatum* datum);

    std::shared_ptr<arrow::DataType> type_;
};

template <typename T, typename A>
void AvroAdaptor::SetValue(int32_t row_idx, const std::shared_ptr<A>& array,
                           ::avro::GenericDatum* datum) {
    if (datum->isUnion()) {
        if (!array->IsNull(row_idx)) {
            datum->selectBranch(1);
            datum->value<T>() = array->Value(row_idx);
        }
    } else {
        datum->value<T>() = array->Value(row_idx);
    }
}

template <>
inline void AvroAdaptor::SetValue<std::string, arrow::StringArray>(
    int32_t row_idx, const std::shared_ptr<arrow::StringArray>& array,
    ::avro::GenericDatum* datum) {
    if (datum->isUnion()) {
        if (!array->IsNull(row_idx)) {
            datum->selectBranch(1);
            datum->value<std::string>() = array->GetString(row_idx);
        }
    } else {
        datum->value<std::string>() = array->GetString(row_idx);
    }
}

template <>
inline void AvroAdaptor::SetValue<std::vector<uint8_t>, arrow::BinaryArray>(
    int32_t row_idx, const std::shared_ptr<arrow::BinaryArray>& array,
    ::avro::GenericDatum* datum) {
    if (datum->isUnion()) {
        if (!array->IsNull(row_idx)) {
            datum->selectBranch(1);
            std::string_view view = array->Value(row_idx);
            datum->value<std::vector<uint8_t>>() = std::vector<uint8_t>(view.begin(), view.end());
        }
    } else {
        std::string_view view = array->Value(row_idx);
        datum->value<std::vector<uint8_t>>() = std::vector<uint8_t>(view.begin(), view.end());
    }
}

}  // namespace paimon::avro
