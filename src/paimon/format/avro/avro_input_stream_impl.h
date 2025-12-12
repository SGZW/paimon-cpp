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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "avro/Stream.hh"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {
class InputStream;
class MemoryPool;
}  // namespace paimon

namespace paimon::avro {

class AvroInputStreamImpl : public ::avro::SeekableInputStream {
 public:
    static Result<std::unique_ptr<AvroInputStreamImpl>> Create(
        const std::shared_ptr<paimon::InputStream>& input_stream, size_t buffer_size,
        const std::shared_ptr<MemoryPool>& pool);

    ~AvroInputStreamImpl() override;

    bool next(const uint8_t** data, size_t* size) override;
    void backup(size_t len) override;
    void skip(size_t len) override;
    size_t byteCount() const override {
        return byte_count_;
    }
    void seek(int64_t position) override;

 private:
    AvroInputStreamImpl(const std::shared_ptr<paimon::InputStream>& input_stream,
                        size_t buffer_size, const uint64_t length,
                        const std::shared_ptr<MemoryPool>& pool);
    bool fill();

    std::shared_ptr<MemoryPool> pool_;
    const size_t buffer_size_;
    const uint64_t total_length_;
    uint8_t* const buffer_;
    std::shared_ptr<paimon::InputStream> in_;
    size_t byte_count_;
    uint8_t* next_;
    size_t available_;
    int32_t total_read_len_ = 0;
};

}  // namespace paimon::avro
