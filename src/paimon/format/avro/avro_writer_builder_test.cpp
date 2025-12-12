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

#include "paimon/format/avro/avro_writer_builder.h"

#include "avro/DataFile.hh"
#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

TEST(ToAvroCompressionKindTest, HandlesValidCompressions) {
    ASSERT_OK_AND_ASSIGN(::avro::Codec zstd_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("zstd"));
    ASSERT_EQ(zstd_codec, ::avro::Codec::ZSTD_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec zstandard_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("zstandard"));
    ASSERT_EQ(zstandard_codec, ::avro::Codec::ZSTD_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec snappy_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("snappy"));
    ASSERT_EQ(snappy_codec, ::avro::Codec::SNAPPY_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec null_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("null"));
    ASSERT_EQ(null_codec, ::avro::Codec::NULL_CODEC);

    ASSERT_OK_AND_ASSIGN(::avro::Codec deflate_codec,
                         AvroWriterBuilder::ToAvroCompressionKind("deflate"));
    ASSERT_EQ(deflate_codec, ::avro::Codec::DEFLATE_CODEC);
}

TEST(ToAvroCompressionKindTest, HandlesInvalidCompression) {
    ASSERT_NOK(AvroWriterBuilder::ToAvroCompressionKind("unknown_compression"));
}

TEST(ToAvroCompressionKindTest, HandlesEmptyString) {
    ASSERT_NOK(AvroWriterBuilder::ToAvroCompressionKind(""));
}
}  // namespace paimon::avro::test
