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

#include "paimon/core/casting/boolean_to_numeric_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

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
BooleanToNumericCastExecutor::BooleanToNumericCastExecutor() {
    literal_cast_executor_map_ = {
        {FieldType::TINYINT,
         [&](const Literal& literal) { return CastLiteral<int8_t>(literal, FieldType::TINYINT); }},
        {FieldType::SMALLINT,
         [&](const Literal& literal) {
             return CastLiteral<int16_t>(literal, FieldType::SMALLINT);
         }},
        {FieldType::INT,
         [&](const Literal& literal) { return CastLiteral<int32_t>(literal, FieldType::INT); }},
        {FieldType::BIGINT,
         [&](const Literal& literal) { return CastLiteral<int64_t>(literal, FieldType::BIGINT); }},
        {FieldType::FLOAT,
         [&](const Literal& literal) { return CastLiteral<float>(literal, FieldType::FLOAT); }},
        {FieldType::DOUBLE,
         [&](const Literal& literal) { return CastLiteral<double>(literal, FieldType::DOUBLE); }}};
}

Result<Literal> BooleanToNumericCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::BOOLEAN);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    auto iter = literal_cast_executor_map_.find(target_field_type);
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(
            fmt::format("cast literal in BooleanToNumericCastExecutor failed: cannot find cast "
                        "function from boolean to {}",
                        target_type->ToString()));
    }
    return iter->second(literal);
}

Result<std::shared_ptr<arrow::Array>> BooleanToNumericCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
