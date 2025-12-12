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
#include <string>
#include <string_view>

#include "avro/GenericDatum.hh"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"

namespace avro {
class GenericRecord;
}  // namespace avro
namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

class AvroRecordDataGetter : public InternalRow {
 public:
    AvroRecordDataGetter(const ::avro::GenericRecord& record,
                         const std::shared_ptr<MemoryPool>& pool);

    bool IsNullAt(int32_t pos) const override;
    bool GetBoolean(int32_t pos) const override;
    char GetByte(int32_t pos) const override;
    int16_t GetShort(int32_t pos) const override;
    int32_t GetInt(int32_t pos) const override;
    int32_t GetDate(int32_t pos) const override;
    int64_t GetLong(int32_t pos) const override;
    float GetFloat(int32_t pos) const override;
    double GetDouble(int32_t pos) const override;
    BinaryString GetString(int32_t pos) const override;
    std::string_view GetStringView(int32_t pos) const override;
    Decimal GetDecimal(int32_t pos, int32_t precision, int32_t scale) const override;
    Timestamp GetTimestamp(int32_t pos, int32_t precision) const override;
    std::shared_ptr<Bytes> GetBinary(int32_t pos) const override;
    std::shared_ptr<InternalArray> GetArray(int32_t pos) const override;
    std::shared_ptr<InternalRow> GetRow(int32_t pos, int32_t num_fields) const override;
    int32_t GetFieldCount() const override;
    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override;
    Result<const RowKind*> GetRowKind() const override;
    void SetRowKind(const RowKind* kind) override;
    std::string ToString() const override;

 private:
    const ::avro::GenericRecord& record_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::avro
