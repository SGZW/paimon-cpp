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

#include "paimon/core/casting/numeric_to_string_cast_executor.h"

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "arrow/compute/cast.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
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

NumericToStringCastExecutor::NumericToStringCastExecutor() {
    literal_cast_executor_map_ = {
        {FieldType::TINYINT, [&](const Literal& literal) { return CastLiteral<int8_t>(literal); }},
        {FieldType::SMALLINT,
         [&](const Literal& literal) { return CastLiteral<int16_t>(literal); }},
        {FieldType::INT, [&](const Literal& literal) { return CastLiteral<int32_t>(literal); }},
        {FieldType::BIGINT, [&](const Literal& literal) { return CastLiteral<int64_t>(literal); }},
        {FieldType::FLOAT, [&](const Literal& literal) { return CastLiteral<float>(literal); }},
        {FieldType::DOUBLE, [&](const Literal& literal) { return CastLiteral<double>(literal); }},
        {FieldType::DECIMAL,
         [&](const Literal& literal) { return CastLiteral<Decimal>(literal); }}};
}

template <typename SrcType>
Literal NumericToStringCastExecutor::CastLiteral(const Literal& literal) {
    if (literal.IsNull()) {
        return Literal(FieldType::STRING);
    }
    auto value = literal.GetValue<SrcType>();
    if constexpr (std::is_same_v<SrcType, float>) {
        arrow::FloatScalar scalar(value);
        std::string string_value = scalar.ToString();
        return Literal(FieldType::STRING, string_value.data(), string_value.size());
    } else if constexpr (std::is_same_v<SrcType, double>) {
        arrow::DoubleScalar scalar(value);
        std::string string_value = scalar.ToString();
        return Literal(FieldType::STRING, string_value.data(), string_value.size());
    } else if constexpr (std::is_same_v<SrcType, Decimal>) {
        auto src_type = arrow::decimal128(value.Precision(), value.Scale());
        arrow::Decimal128Scalar scalar(arrow::Decimal128(value.HighBits(), value.LowBits()),
                                       src_type);
        std::string string_value = scalar.ToString();
        return Literal(FieldType::STRING, string_value.data(), string_value.size());
    } else {
        std::string string_value = std::to_string(value);
        return Literal(FieldType::STRING, string_value.data(), string_value.size());
    }
}

Result<Literal> NumericToStringCastExecutor::Cast(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& target_type) const {
    assert(target_type->id() == arrow::Type::type::STRING);
    FieldType src_type = literal.GetType();
    auto iter = literal_cast_executor_map_.find(src_type);
    if (iter == literal_cast_executor_map_.end()) {
        return Status::Invalid(
            fmt::format("cast literal in NumericToStringCastExecutor failed: cannot find cast "
                        "function from {} to STRING",
                        FieldTypeUtils::FieldTypeToString(src_type)));
    }
    return iter->second(literal);
}

Result<std::shared_ptr<arrow::Array>> NumericToStringCastExecutor::Cast(
    const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& target_type,
    arrow::MemoryPool* pool) const {
    arrow::compute::CastOptions options = arrow::compute::CastOptions::Safe();
    return CastingUtils::Cast(array, target_type, options, pool);
}

}  // namespace paimon
