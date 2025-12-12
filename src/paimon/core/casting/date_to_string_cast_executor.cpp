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

#include "paimon/core/casting/date_to_string_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>

#include "arrow/compute/cast.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/defs.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> DateToStringCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(target_type->id() == arrow::Type::type::STRING);
    assert(literal.GetType() == FieldType::DATE);
    return CastingUtils::Cast<arrow::Date32Scalar, int32_t, arrow::StringScalar, std::string>(
        literal, arrow::date32(), target_type, arrow::compute::CastOptions::Safe());
}

Result<std::shared_ptr<arrow::Array>> DateToStringCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
