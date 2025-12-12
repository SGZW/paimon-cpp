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

#include "paimon/format/avro/avro_array_data_getter.h"

#include "avro/GenericDatum.hh"
#include "paimon/format/avro/avro_datum_data_getter.h"

namespace paimon::avro {

bool AvroArrayDataGetter::IsNullAt(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::IsNullAt(array_.value()[pos]);
}
bool AvroArrayDataGetter::GetBoolean(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetBoolean(array_.value()[pos]);
}
char AvroArrayDataGetter::GetByte(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetByte(array_.value()[pos]);
}
int16_t AvroArrayDataGetter::GetShort(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetShort(array_.value()[pos]);
}
int32_t AvroArrayDataGetter::GetInt(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetInt(array_.value()[pos]);
}
int32_t AvroArrayDataGetter::GetDate(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetDate(array_.value()[pos]);
}
int64_t AvroArrayDataGetter::GetLong(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetLong(array_.value()[pos]);
}
float AvroArrayDataGetter::GetFloat(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetFloat(array_.value()[pos]);
}
double AvroArrayDataGetter::GetDouble(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetDouble(array_.value()[pos]);
}
BinaryString AvroArrayDataGetter::GetString(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetString(array_.value()[pos], pool_);
}
std::string_view AvroArrayDataGetter::GetStringView(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetStringView(array_.value()[pos]);
}
Decimal AvroArrayDataGetter::GetDecimal(int32_t pos, int32_t precision, int32_t scale) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetDecimal(array_.value()[pos], precision, scale, pool_);
}
Timestamp AvroArrayDataGetter::GetTimestamp(int32_t pos, int32_t precision) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetTimestamp(array_.value()[pos], precision);
}
std::shared_ptr<Bytes> AvroArrayDataGetter::GetBinary(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetBinary(array_.value()[pos], pool_);
}

std::shared_ptr<InternalArray> AvroArrayDataGetter::GetArray(int32_t pos) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetArray(array_.value()[pos], pool_);
}

std::shared_ptr<InternalRow> AvroArrayDataGetter::GetRow(int32_t pos, int32_t num_fields) const {
    assert(pos < Size());
    return AvroDatumDataGetter::GetRow(array_.value()[pos], num_fields, pool_);
}

}  // namespace paimon::avro
