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

#include "paimon/format/avro/avro_record_converter.h"

#include <cassert>
#include <cstddef>
#include <functional>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/util/checked_cast.h"
#include "avro/GenericDatum.hh"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/format/avro/avro_record_data_getter.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

Result<std::unique_ptr<AvroRecordConverter>> AvroRecordConverter::Create(
    const std::shared_ptr<::arrow::DataType>& type, const std::shared_ptr<MemoryPool>& pool) {
    auto arrow_pool = GetArrowPool(pool);
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::MakeBuilder(arrow_pool.get(), type, &array_builder));

    auto struct_builder =
        arrow::internal::checked_pointer_cast<arrow::StructBuilder>(std::move(array_builder));
    assert(struct_builder);
    std::vector<RowToArrowArrayConverter::AppendValueFunc> appenders;
    // first is the root struct array
    int32_t reserve_count = 1;
    for (size_t i = 0; i < type->fields().size(); i++) {
        PAIMON_ASSIGN_OR_RAISE(
            RowToArrowArrayConverter::AppendValueFunc func,
            AppendField(/*use_view=*/true, struct_builder->field_builder(i), &reserve_count));
        appenders.emplace_back(func);
    }
    return std::unique_ptr<AvroRecordConverter>(
        new AvroRecordConverter(reserve_count, std::move(appenders), std::move(struct_builder),
                                std::move(arrow_pool), type, pool));
}

Result<BatchReader::ReadBatch> AvroRecordConverter::NextBatch(
    const std::vector<::avro::GenericDatum>& avro_datums) {
    PAIMON_RETURN_NOT_OK(ResetAndReserve());
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder_->AppendValues(avro_datums.size(), /*valid_bytes=*/nullptr));
    const auto& fields = type_->fields();
    for (size_t i = 0; i < fields.size(); i++) {
        for (const auto& avro_datum : avro_datums) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(appenders_[i](
                AvroRecordDataGetter(avro_datum.value<::avro::GenericRecord>(), pool_), i));
        }
    }
    return FinishAndAccumulate();
}

AvroRecordConverter::AvroRecordConverter(int32_t reserve_count,
                                         std::vector<AppendValueFunc>&& appenders,
                                         std::unique_ptr<arrow::StructBuilder>&& array_builder,
                                         std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                                         const std::shared_ptr<::arrow::DataType>& type,
                                         const std::shared_ptr<MemoryPool>& pool)
    : RowToArrowArrayConverter(reserve_count, std::move(appenders), std::move(array_builder),
                               std::move(arrow_pool)),
      type_(type),
      pool_(pool) {}

}  // namespace paimon::avro
