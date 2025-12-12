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
#include <vector>

#include "avro/GenericDatum.hh"
#include "paimon/core/io/row_to_arrow_array_converter.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class MemoryPool;
class StructBuilder;
}  // namespace arrow
namespace avro {
class GenericDatum;
}  // namespace avro
namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

class AvroRecordConverter
    : public RowToArrowArrayConverter<::avro::GenericDatum, BatchReader::ReadBatch> {
 public:
    static Result<std::unique_ptr<AvroRecordConverter>> Create(
        const std::shared_ptr<::arrow::DataType>& type, const std::shared_ptr<MemoryPool>& pool);

    Result<BatchReader::ReadBatch> NextBatch(
        const std::vector<::avro::GenericDatum>& avro_datums) override;

 private:
    AvroRecordConverter(int32_t reserve_count, std::vector<AppendValueFunc>&& appenders,
                        std::unique_ptr<arrow::StructBuilder>&& array_builder,
                        std::unique_ptr<arrow::MemoryPool>&& arrow_pool,
                        const std::shared_ptr<::arrow::DataType>& type,
                        const std::shared_ptr<MemoryPool>& pool);

    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::avro
