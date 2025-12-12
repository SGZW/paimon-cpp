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
#include <optional>
#include <utility>
#include <vector>

#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
/// A `MergeFunction` for lookup, this wrapper only considers the latest high level record,
/// because each merge will query the old merged record, so the latest high level record should be
/// the final merged value.
class LookupMergeFunction : public MergeFunction {
 public:
    explicit LookupMergeFunction(std::unique_ptr<MergeFunction>&& merge_function)
        : merge_function_(std::move(merge_function)) {}

    void Reset() override {
        candidates_.clear();
    }

    Status Add(KeyValue&& kv) override {
        candidates_.emplace_back(std::move(kv));
        return Status::OK();
    }

    Result<std::optional<KeyValue>> GetResult() override {
        // 1. Find the latest high level record
        bool has_high_level = false;
        std::vector<KeyValue> target_candidates;
        target_candidates.reserve(candidates_.size());
        for (int32_t i = static_cast<int32_t>(candidates_.size()) - 1; i >= 0; i--) {
            if (candidates_[i].level > 0) {
                if (has_high_level) {
                    continue;
                } else {
                    has_high_level = true;
                }
            }
            target_candidates.emplace_back(std::move(candidates_[i]));
        }
        // 2. Do the merge for inputs
        merge_function_->Reset();
        // step 1 visits candidates_ from end to begin, therefore elements in target_candidates need
        // to be reversely accessed
        for (int32_t i = static_cast<int32_t>(target_candidates.size()) - 1; i >= 0; i--) {
            PAIMON_RETURN_NOT_OK(merge_function_->Add(std::move(target_candidates[i])));
        }
        candidates_.clear();
        return merge_function_->GetResult();
    }

 private:
    std::unique_ptr<MergeFunction> merge_function_;
    std::vector<KeyValue> candidates_;
};
}  // namespace paimon
