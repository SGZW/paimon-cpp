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

#include <functional>
#include <memory>

#include "paimon/core/io/data_file_meta.h"

namespace paimon {
/// Compact manager for `AppendOnlyFileStore`.
class BucketedAppendCompactManager {
 public:
    BucketedAppendCompactManager() = delete;
    ~BucketedAppendCompactManager() = delete;

    /// New files may be created during the compaction process, then the results of the compaction
    /// may be put after the new files, and this order will be disrupted. We need to ensure this
    /// order, so we force the order by sequence.
    static std::function<bool(const std::shared_ptr<DataFileMeta>&,
                              const std::shared_ptr<DataFileMeta>&)>
    FileComparator(bool ignore_overlap) {
        return [ignore_overlap](const std::shared_ptr<DataFileMeta>& lhs,
                                const std::shared_ptr<DataFileMeta>& rhs) -> bool {
            if (*lhs == *rhs) {
                return false;
            }
            if (!ignore_overlap && IsOverlap(lhs, rhs)) {
                // TODO(yonghao.fyh): add log
            }
            return lhs->min_sequence_number < rhs->min_sequence_number;
        };
    }

 private:
    static bool IsOverlap(const std::shared_ptr<DataFileMeta>& o1,
                          const std::shared_ptr<DataFileMeta>& o2) {
        return o2->min_sequence_number <= o1->max_sequence_number &&
               o2->max_sequence_number >= o1->min_sequence_number;
    }
};
}  // namespace paimon
