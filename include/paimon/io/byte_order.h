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
namespace paimon {

#if defined(__s390x__)
#define PAIMON_LITTLEENDIAN 0
#endif  // __s390x__
#if !defined(PAIMON_LITTLEENDIAN)
#if defined(__GNUC__) || defined(__clang__) || defined(__ICCARM__)
#if (defined(__BIG_ENDIAN__) || (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__))
#define PAIMON_LITTLEENDIAN 0
#else
#define PAIMON_LITTLEENDIAN 1
#endif  // __BIG_ENDIAN__
#elif defined(_MSC_VER)
#if defined(_M_PPC)
#define PAIMON_LITTLEENDIAN 0
#else
#define PAIMON_LITTLEENDIAN 1
#endif
#else
#error Unable to determine endianness, define PAIMON_LITTLEENDIAN.
#endif
#endif  // !defined(PAIMON_LITTLEENDIAN)

enum class ByteOrder : int8_t { PAIMON_BIG_ENDIAN = 1, PAIMON_LITTLE_ENDIAN = 2 };

/// Get the byte order of the system.
constexpr ByteOrder SystemByteOrder() {
    if (PAIMON_LITTLEENDIAN) {
        return ByteOrder::PAIMON_LITTLE_ENDIAN;
    } else {
        return ByteOrder::PAIMON_BIG_ENDIAN;
    }
}

}  // namespace paimon
