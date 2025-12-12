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

#include <map>
#include <memory>
#include <string>

#include "paimon/factories/factory.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
class FileIndexer;

/// File index factory to construct `FileIndexer`.
class PAIMON_EXPORT FileIndexerFactory : public Factory {
 public:
    ~FileIndexerFactory() override;

    /// Get a `FileIndexer` according to identifier and options.
    static Result<std::unique_ptr<FileIndexer>> Get(
        const std::string& identifier, const std::map<std::string, std::string>& options);

    /// Create a `FileIndexer` with specified options.
    virtual Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const = 0;
};

}  // namespace paimon
