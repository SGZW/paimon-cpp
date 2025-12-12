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

#include <memory>
#include <string>

#include "paimon/type_fwd.h"

namespace paimon {
/// Create a file format writer based on the file output stream. Allows you to specify memory pool.
class PAIMON_EXPORT WriterBuilder {
 public:
    virtual ~WriterBuilder() = default;

    /// Set memory pool to use.
    virtual WriterBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) = 0;

    /// Build a file format writer based on the file output stream and file compression.
    virtual Result<std::unique_ptr<FormatWriter>> Build(const std::shared_ptr<OutputStream>& out,
                                                        const std::string& compression) = 0;
};

/// Create a file format writer based on the file path.
class PAIMON_EXPORT DirectWriterBuilder : public WriterBuilder {
 public:
    ~DirectWriterBuilder() override = default;
    virtual Result<std::unique_ptr<FormatWriter>> BuildFromPath(const std::string& path) = 0;
};

/// `SpecificFSWriterBuilder` allows you to specify a specific file system.
class PAIMON_EXPORT SpecificFSWriterBuilder : public WriterBuilder {
 public:
    ~SpecificFSWriterBuilder() override = default;
    // Set file system to use.
    virtual SpecificFSWriterBuilder* WithFileSystem(const std::shared_ptr<FileSystem>& fs) = 0;
};

}  // namespace paimon
