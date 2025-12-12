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

#include "paimon/core/casting/numeric_to_boolean_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
NumericToBooleanCastExecutor::NumericToBooleanCastExecutor() {
    literal_cast_executor_map_ = {
        {FieldType::TINYINT, [&](const Literal& literal) { return CastLiteral<int8_t>(literal); }},
        {FieldType::SMALLINT,
         [&](const Literal& literal) { return CastLiteral<int16_t>(literal); }},
        {FieldType::INT, [&](const Literal& literal) { return CastLiteral<int32_t>(literal); }},
        {FieldType::BIGINT, [&](const Literal& literal) { return CastLiteral<int64_t>(literal); }}};
}

Result<Literal> NumericToBooleanCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(target_type->id() == arrow::Type::type::BOOL);
    FieldType src_type = literal.GetType();
    auto iter = literal_cast_executor_map_.find(src_type);
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(
            fmt::format("cast literal in NumericToBooleanCastExecutor failed: cannot find cast "
                        "function from {} to boolean",
                        FieldTypeUtils::FieldTypeToString(src_type)));
    }
    return iter->second(literal);
}

Result<std::shared_ptr<arrow::Array>> NumericToBooleanCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
