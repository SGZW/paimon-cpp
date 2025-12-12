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

#include "paimon/format/avro/avro_record_data_getter.h"

#include <cassert>

#include "avro/GenericDatum.hh"
#include "paimon/format/avro/avro_datum_data_getter.h"
#include "paimon/status.h"

namespace paimon {
class InternalMap;
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

AvroRecordDataGetter::AvroRecordDataGetter(const ::avro::GenericRecord& record,
                                           const std::shared_ptr<MemoryPool>& pool)
    : record_(record), pool_(pool) {}

bool AvroRecordDataGetter::IsNullAt(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::IsNullAt(record_.fieldAt(pos));
}
bool AvroRecordDataGetter::GetBoolean(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetBoolean(record_.fieldAt(pos));
}
char AvroRecordDataGetter::GetByte(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetByte(record_.fieldAt(pos));
}
int16_t AvroRecordDataGetter::GetShort(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetShort(record_.fieldAt(pos));
}
int32_t AvroRecordDataGetter::GetInt(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetInt(record_.fieldAt(pos));
}
int32_t AvroRecordDataGetter::GetDate(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetDate(record_.fieldAt(pos));
}
int64_t AvroRecordDataGetter::GetLong(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetLong(record_.fieldAt(pos));
}
float AvroRecordDataGetter::GetFloat(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetFloat(record_.fieldAt(pos));
}
double AvroRecordDataGetter::GetDouble(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetDouble(record_.fieldAt(pos));
}
BinaryString AvroRecordDataGetter::GetString(int32_t pos) const {
    assert(false);
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetString(record_.fieldAt(pos), pool_);
}
std::string_view AvroRecordDataGetter::GetStringView(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetStringView(record_.fieldAt(pos));
}
Decimal AvroRecordDataGetter::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetDecimal(record_.fieldAt(pos), precision, scale, pool_);
}
Timestamp AvroRecordDataGetter::GetTimestamp(int32_t pos, int32_t precision) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetTimestamp(record_.fieldAt(pos), precision);
}
std::shared_ptr<Bytes> AvroRecordDataGetter::GetBinary(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetBinary(record_.fieldAt(pos), pool_);
}

std::shared_ptr<InternalArray> AvroRecordDataGetter::GetArray(int32_t pos) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetArray(record_.fieldAt(pos), pool_);
}

std::shared_ptr<InternalRow> AvroRecordDataGetter::GetRow(int32_t pos, int32_t num_fields) const {
    assert(pos < GetFieldCount());
    return AvroDatumDataGetter::GetRow(record_.fieldAt(pos), num_fields, pool_);
}

int32_t AvroRecordDataGetter::GetFieldCount() const {
    return record_.fieldCount();
}

std::shared_ptr<InternalMap> AvroRecordDataGetter::GetMap(int32_t pos) const {
    assert(false);
    return nullptr;
}

Result<const RowKind*> AvroRecordDataGetter::GetRowKind() const {
    assert(false);
    return Status::Invalid("avro record do not have row kind.");
}

void AvroRecordDataGetter::SetRowKind(const RowKind* kind) {
    assert(false);
}

std::string AvroRecordDataGetter::ToString() const {
    assert(false);
    return "";
}

}  // namespace paimon::avro
