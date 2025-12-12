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

#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {
/// File index result to decide whether filter a file.
class PAIMON_EXPORT FileIndexResult : public std::enable_shared_from_this<FileIndexResult> {
 public:
    virtual ~FileIndexResult() = default;

    /// @return A shared instance representing "retain the file".
    /// @note This is a singleton-like utility; all calls return equivalent objects.
    static std::shared_ptr<FileIndexResult> Remain();

    /// @return A shared instance representing "skip the file".
    /// @note This is a singleton-like utility; all calls return equivalent objects.
    static std::shared_ptr<FileIndexResult> Skip();

    /// @return Whether the file is remained.
    virtual Result<bool> IsRemain() const = 0;

    /// Compute the intersection of the current result with the provided result.
    virtual Result<std::shared_ptr<FileIndexResult>> And(
        const std::shared_ptr<FileIndexResult>& other);

    /// Compute the union of the current result with the provided result.
    virtual Result<std::shared_ptr<FileIndexResult>> Or(
        const std::shared_ptr<FileIndexResult>& other);

    virtual std::string ToString() const = 0;
};

/// Concrete implementation of FileIndexResult that always retains the file.
class PAIMON_EXPORT Remain : public FileIndexResult {
 public:
    Result<bool> IsRemain() const override;
    Result<std::shared_ptr<FileIndexResult>> And(
        const std::shared_ptr<FileIndexResult>& other) override;
    Result<std::shared_ptr<FileIndexResult>> Or(
        const std::shared_ptr<FileIndexResult>& other) override;
    std::string ToString() const override;
};

/// Concrete implementation of FileIndexResult that always skips the file.
class PAIMON_EXPORT Skip : public FileIndexResult {
 public:
    Result<bool> IsRemain() const override;
    Result<std::shared_ptr<FileIndexResult>> And(
        const std::shared_ptr<FileIndexResult>& other) override;
    Result<std::shared_ptr<FileIndexResult>> Or(
        const std::shared_ptr<FileIndexResult>& other) override;
    std::string ToString() const override;
};

}  // namespace paimon
