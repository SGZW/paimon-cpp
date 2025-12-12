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

#include <cassert>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "avro/GenericDatum.hh"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class InternalMap;
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

class AvroArrayDataGetter : public InternalArray {
 public:
    AvroArrayDataGetter(const ::avro::GenericArray& array, const std::shared_ptr<MemoryPool>& pool)
        : array_(array), pool_(pool) {}

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

    int32_t Size() const override {
        return array_.value().size();
    }
    std::shared_ptr<InternalMap> GetMap(int32_t pos) const override {
        assert(false);
        return nullptr;
    }
    Result<std::vector<char>> ToBooleanArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<char>> ToByteArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<int16_t>> ToShortArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<int32_t>> ToIntArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<int64_t>> ToLongArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<float>> ToFloatArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }
    Result<std::vector<double>> ToDoubleArray() const override {
        assert(false);
        return Status::NotImplemented("not implemented");
    }

 private:
    const ::avro::GenericArray& array_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::avro
