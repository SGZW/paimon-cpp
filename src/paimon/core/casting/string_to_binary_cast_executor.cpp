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

#include "paimon/core/casting/string_to_binary_cast_executor.h"

#include <cassert>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/type.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> StringToBinaryCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::STRING);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    assert(target_field_type == FieldType::BINARY);
    if (literal.IsNull()) {
        return Literal(target_field_type);
    }
    auto value = literal.GetValue<std::string>();
    return Literal(FieldType::BINARY, value.data(), value.size());
}

Result<std::shared_ptr<arrow::Array>> StringToBinaryCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
