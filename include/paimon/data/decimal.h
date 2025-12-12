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
#include <cstring>
#include <string>
#include <vector>

#include "paimon/visibility.h"

namespace paimon {
class Bytes;

/// A data structure representing data of Decimal. It might be stored in a compact representation
/// (as a long value) if values are small enough.
class PAIMON_EXPORT Decimal {
 public:
    using int128_t = __int128_t;
    using uint128_t = __uint128_t;

    Decimal(int32_t precision, int32_t scale, int128_t value)
        : precision_(precision), scale_(scale), value_(value) {}

    /// Get the **precision** of this decimal.
    ///
    /// The precision is the number of digits in the unscaled value.
    int32_t Precision() const {
        return precision_;
    }

    /// Get the **scale** of this decimal.
    int32_t Scale() const {
        return scale_;
    }

    /// Get the underlying int128_t value of this decimal.
    int128_t Value() const {
        return value_;
    }

    /// Get the low 64 bits of the decimal value.
    uint64_t LowBits() const {
        return static_cast<uint64_t>(value_ & 0xFFFFFFFFFFFFFFFF);
    }

    /// Get the high 64 bits of the decimal value.
    uint64_t HighBits() const {
        return static_cast<uint64_t>(value_ >> 64);
    }

    /// @return Whether the decimal value is small enough to be stored in a long.
    bool IsCompact() const {
        return precision_ <= MAX_COMPACT_PRECISION;
    }

    std::string ToString() const;

    /// @return Whether the decimal value is small enough to be stored in a long.
    static bool IsCompact(int32_t precision) {
        return precision <= MAX_COMPACT_PRECISION;
    }

    /// Creates an instance of `Decimal` from an unscaled long value and the given precision and
    /// scale.
    static Decimal FromUnscaledLong(int64_t unscaled_long, int32_t precision, int32_t scale) {
        return {precision, scale, unscaled_long};
    }

    /// @return A long describing the **unscaled value** of this decimal.
    int64_t ToUnscaledLong() const {
        int64_t ret = 0;
        uint64_t low_bits = (value_ & 0xFFFFFFFFFFFFFFFF);
        memcpy(&ret, &low_bits, sizeof(uint64_t));
        return ret;
    }

    /// @return A byte array describing the **unscaled value** of this decimal.
    std::vector<char> ToUnscaledBytes() const;

    /// Creates an instance of `Decimal` from an unscaled byte array value and the given precision
    /// and scale.
    static Decimal FromUnscaledBytes(int32_t precision, int32_t scale, Bytes* bytes);

    bool operator==(const Decimal& other) const {
        if (this == &other) {
            return true;
        }
        return CompareTo(other) == 0;
    }

    bool operator<(const Decimal& other) const {
        if (this == &other) {
            return false;
        }
        return CompareTo(other) < 0;
    }

    bool operator>(const Decimal& other) const {
        if (this == &other) {
            return false;
        }
        return CompareTo(other) > 0;
    }

    int32_t CompareTo(const Decimal& other) const;
    static constexpr int32_t DEFAULT_PRECISION = 10;
    static constexpr int32_t DEFAULT_SCALE = 0;
    static constexpr int32_t MIN_PRECISION = 1;
    static constexpr int32_t MAX_PRECISION = 38;

 private:
    static constexpr int32_t MAX_COMPACT_PRECISION = 18;
    static const int128_t INT128_MAXIMUM_VALUE;
    static const int128_t INT128_MINIMUM_VALUE;
    static const int64_t POWERS_OF_TEN[MAX_COMPACT_PRECISION + 1];

    static int32_t clz_u128(uint128_t u);
    static int32_t count_leading_zero_bytes(uint128_t u);
    static int32_t count_leading_all_ones_bytes(uint128_t u);
    static int128_t DownScaleInt128(int128_t value, int32_t scale);
    static int128_t ScaleInt128(int128_t value, int32_t scale, bool* overflow);

 private:
    int32_t precision_ = 0;
    int32_t scale_ = 0;
    int128_t value_ = 0;
};

}  // namespace paimon
