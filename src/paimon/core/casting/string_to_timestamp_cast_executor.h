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

#include <memory>

#include "arrow/array/array_base.h"
#include "paimon/core/casting/cast_executor.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class MemoryPool;
}  // namespace arrow

namespace paimon {
class StringToTimestampCastExecutor : public CastExecutor {
 public:
    Result<Literal> Cast(const Literal& literal,
                         const std::shared_ptr<arrow::DataType>& target_type) const override;

    // Noted that: 1.Java Paimon can cast timestamp range [0000-01-01 00:00:00.00000000, 9999-12-31
    // 23:59:59.999999999] while C++ Paimon can cast timestamp range [min_int64, max_int64] while
    // target type is nano, ns value beyond C++ Paimon range will be undefined behavior; 2.Java
    // Paimon supports passing a numeric value in TimestampType while C++ Paimon does not; 3. C++
    // Paimon supports the format "1970-01-01T00:00:00" while Java paimon does not
    Result<std::shared_ptr<arrow::Array>> Cast(const std::shared_ptr<arrow::Array>& array,
                                               const std::shared_ptr<arrow::DataType>& target_type,
                                               arrow::MemoryPool* pool) const override;
};

}  // namespace paimon
