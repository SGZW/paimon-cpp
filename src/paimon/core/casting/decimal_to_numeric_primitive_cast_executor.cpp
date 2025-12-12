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

#include "paimon/core/casting/decimal_to_numeric_primitive_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/casting/casting_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/status.h"

namespace arrow {
class MemoryPool;
class Array;
}  // namespace arrow

namespace paimon {
Result<Literal> DecimalToNumericPrimitiveCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(literal.GetType() == FieldType::DECIMAL);
    PAIMON_ASSIGN_OR_RAISE(FieldType target_field_type,
                           FieldTypeUtils::ConvertToFieldType(target_type->id()));
    if (literal.IsNull()) {
        PAIMON_ASSIGN_OR_RAISE(FieldType type,
                               FieldTypeUtils::ConvertToFieldType(target_type->id()));
        return Literal(type);
    }
    auto decimal_value = literal.GetValue<Decimal>();
    auto src_type = arrow::decimal128(decimal_value.Precision(), decimal_value.Scale());
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    options.allow_decimal_truncate = true;
    options.allow_int_overflow = true;
    if (target_field_type == FieldType::TINYINT) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::Int8Type>, int8_t>(
            literal, src_type, target_type, options);
    } else if (target_field_type == FieldType::SMALLINT) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::Int16Type>, int16_t>(
            literal, src_type, target_type, options);
    } else if (target_field_type == FieldType::INT) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::Int32Type>, int32_t>(
            literal, src_type, target_type, options);
    } else if (target_field_type == FieldType::BIGINT) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::Int64Type>, int64_t>(
            literal, src_type, target_type, options);
    } else if (target_field_type == FieldType::FLOAT) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::FloatType>, float>(
            literal, src_type, target_type, options);
    } else if (target_field_type == FieldType::DOUBLE) {
        return CastingUtils::Cast<arrow::Decimal128Scalar, Decimal,
                                  arrow::NumericScalar<arrow::DoubleType>, double>(
            literal, src_type, target_type, options);
    }
    return Status::Invalid(fmt::format(
        "cast literal in DecimalToNumericPrimitiveCastExecutor failed: cannot find cast "
        "function from decimal to {}",
        FieldTypeUtils::FieldTypeToString(target_field_type)));
}

Result<std::shared_ptr<arrow::Array>> DecimalToNumericPrimitiveCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    options.allow_decimal_truncate = true;
    options.allow_int_overflow = true;
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
