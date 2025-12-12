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

#include "paimon/format/avro/avro_adaptor.h"

#include <cstddef>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/util/checked_cast.h"
#include "avro/GenericDatum.hh"
#include "avro/Node.hh"
#include "avro/ValidSchema.hh"
#include "paimon/status.h"

namespace paimon::avro {

Result<std::vector<::avro::GenericDatum>> AvroAdaptor::ConvertArrayToGenericDatums(
    const std::shared_ptr<arrow::Array>& array, const ::avro::ValidSchema& avro_schema) const {
    std::vector<::avro::GenericDatum> datums;
    for (int32_t i = 0; i < array->length(); i++) {
        PAIMON_ASSIGN_OR_RAISE(::avro::GenericDatum datum,
                               ConvertArrayToGenericDatum(avro_schema, array, i));
        datums.push_back(datum);
    }
    return datums;
}

Result<::avro::GenericDatum> AvroAdaptor::ConvertArrayToGenericDatum(
    const ::avro::ValidSchema& avro_schema, const std::shared_ptr<arrow::Array>& arrow_array,
    int32_t row_idx) const {
    std::shared_ptr<arrow::StructArray> struct_array =
        arrow::internal::checked_pointer_cast<arrow::StructArray>(arrow_array);
    if (struct_array == nullptr) {
        return Status::Invalid("arrow array should be struct array");
    }
    ::avro::GenericDatum datum(avro_schema.root());
    auto& record = datum.value<::avro::GenericRecord>();
    const auto& node = avro_schema.root();
    const auto& fields = type_->fields();
    for (size_t i = 0; i < fields.size(); i++) {
        std::shared_ptr<arrow::Array> field_array = struct_array->field(i);
        switch (fields[i]->type()->id()) {
            case arrow::Type::type::BOOL: {
                auto array =
                    arrow::internal::checked_pointer_cast<arrow::BooleanArray>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<bool, arrow::BooleanArray>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::INT8: {
                auto array = arrow::internal::checked_pointer_cast<arrow::Int8Array>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                // avro only support int32_t and int64_t
                SetValue<int32_t, arrow::Int8Array>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::INT16: {
                auto array = arrow::internal::checked_pointer_cast<arrow::Int16Array>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                // avro only support int32_t and int64_t
                SetValue<int32_t, arrow::Int16Array>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::INT32: {
                auto array = arrow::internal::checked_pointer_cast<arrow::Int32Array>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<int32_t, arrow::Int32Array>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::INT64: {
                auto array = arrow::internal::checked_pointer_cast<arrow::Int64Array>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<int64_t, arrow::Int64Array>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::FLOAT: {
                auto array = arrow::internal::checked_pointer_cast<arrow::FloatArray>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<float, arrow::FloatArray>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::DOUBLE: {
                auto array = arrow::internal::checked_pointer_cast<arrow::DoubleArray>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<double, arrow::DoubleArray>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::STRING: {
                auto array = arrow::internal::checked_pointer_cast<arrow::StringArray>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<std::string, arrow::StringArray>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::BINARY: {
                auto array = arrow::internal::checked_pointer_cast<arrow::BinaryArray>(field_array);
                auto datum = ::avro::GenericDatum(avro_schema.root()->leafAt(i));
                SetValue<std::vector<uint8_t>, arrow::BinaryArray>(row_idx, array, &datum);
                record.setFieldAt(i, datum);
                break;
            }
            case arrow::Type::type::STRUCT: {
                ::avro::ValidSchema leaf_schema(node->leafAt(i));
                PAIMON_ASSIGN_OR_RAISE(
                    ::avro::GenericDatum datum,
                    ConvertArrayToGenericDatum(leaf_schema, field_array, row_idx));
                record.setFieldAt(i, datum);
                break;
            }
            default: {
                return Status::Invalid("unsupported type");
            }
        }
    }
    return datum;
}

}  // namespace paimon::avro
