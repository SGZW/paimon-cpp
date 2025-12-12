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

#include "paimon/core/casting/numeric_primitive_cast_executor.h"

#include <cstdint>
#include <string>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
NumericPrimitiveCastExecutor::NumericPrimitiveCastExecutor() {
    literal_cast_executor_map_ = {
        {std::make_pair(FieldType::TINYINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::TINYINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int8_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::SMALLINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::SMALLINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int16_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::INT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::INT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::INT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::INT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int32_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::BIGINT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::BIGINT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<int64_t, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::FLOAT, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<float, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<float, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<float, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::FLOAT, FieldType::DOUBLE),
         [&](const Literal& literal) {
             return CastLiteral<float, double>(literal, FieldType::DOUBLE);
         }},

        {std::make_pair(FieldType::DOUBLE, FieldType::TINYINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int8_t>(literal, FieldType::TINYINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::SMALLINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int16_t>(literal, FieldType::SMALLINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::INT),
         [&](const Literal& literal) {
             return CastLiteral<double, int32_t>(literal, FieldType::INT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::BIGINT),
         [&](const Literal& literal) {
             return CastLiteral<double, int64_t>(literal, FieldType::BIGINT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::FLOAT),
         [&](const Literal& literal) {
             return CastLiteral<double, float>(literal, FieldType::FLOAT);
         }},
        {std::make_pair(FieldType::DOUBLE, FieldType::DOUBLE), [&](const Literal& literal) {
             return CastLiteral<double, double>(literal, FieldType::DOUBLE);
         }}};
}

Result<Literal> NumericPrimitiveCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    FieldType src_type = literal.GetType();
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    auto iter = literal_cast_executor_map_.find(std::make_pair(src_type, target_field_type));
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(
            fmt::format("cast literal in NumericPrimitiveCastExecutor failed: cannot find cast "
                        "function from {} to {}",
                        FieldTypeUtils::FieldTypeToString(src_type),
                        FieldTypeUtils::FieldTypeToString(target_field_type)));
    }
    return iter->second(literal);
}

Result<std::shared_ptr<arrow::Array>> NumericPrimitiveCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    options.allow_int_overflow = true;
    options.allow_float_truncate = true;
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
