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

#include "arrow/array/array_base.h"
#include "arrow/util/decimal.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class MemoryPool;
}  // namespace arrow

namespace paimon {
class StringToDecimalCastExecutor : public CastExecutor {
 public:
    Result<Literal> Cast(const Literal& literal,
                         const std::shared_ptr<arrow::DataType>& target_type) const override;

    Result<std::shared_ptr<arrow::Array>> Cast(const std::shared_ptr<arrow::Array>& array,
                                               const std::shared_ptr<arrow::DataType>& target_type,
                                               arrow::MemoryPool* pool) const override;

 private:
    static Result<std::optional<arrow::Decimal128>> StringToDecimal(const std::string& str_value,
                                                                    int32_t target_precision,
                                                                    int32_t target_scale);
};

}  // namespace paimon
