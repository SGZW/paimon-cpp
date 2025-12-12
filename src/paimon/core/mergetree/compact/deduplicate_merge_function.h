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
#include <optional>
#include <utility>

#include "paimon/common/types/row_kind.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// A `MergeFunction` where key is primary key (unique) and value is the full record, only keep
/// the latest one.
class DeduplicateMergeFunction : public MergeFunction {
 public:
    explicit DeduplicateMergeFunction(bool ignore_delete) : ignore_delete_(ignore_delete) {}

    void Reset() override {
        latest_kv_ = std::nullopt;
    }

    Status Add(KeyValue&& kv) override {
        // In 0.7- versions, the delete records might be written into data file even when
        // ignore-delete configured, so ignoreDelete still needs to be checked
        if (ignore_delete_ && kv.value_kind->IsRetract()) {
            return Status::OK();
        }
        latest_kv_ = std::move(kv);
        return Status::OK();
    }

    Result<std::optional<KeyValue>> GetResult() override {
        return std::move(latest_kv_);
    }

 private:
    bool ignore_delete_;
    std::optional<KeyValue> latest_kv_;
};
}  // namespace paimon
