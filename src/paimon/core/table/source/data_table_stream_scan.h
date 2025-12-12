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

#include "paimon/core/snapshot.h"
#include "paimon/core/table/source/abstract_table_scan.h"
#include "paimon/core/table/source/plan_impl.h"
#include "paimon/core/table/source/snapshot/follow_up_scanner.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"

namespace paimon {
class CoreOptions;
class FollowUpScanner;
class SnapshotReader;
class StartingScanner;

/// `StreamTableScan` implementation for streaming planning.
class DataTableStreamScan : public AbstractTableScan {
 public:
    DataTableStreamScan(const CoreOptions& core_options,
                        const std::shared_ptr<SnapshotReader>& snapshot_reader);

    Result<std::shared_ptr<Plan>> CreatePlan() override;

 private:
    Status InitScanner();

    Result<std::shared_ptr<Plan>> TryFirstPlan();

    Result<std::shared_ptr<Plan>> NextPlan();

    Result<std::optional<Snapshot>> GetNextSnapshot(int64_t next_snapshot_id) const;

 private:
    std::shared_ptr<StartingScanner> starting_scanner_;
    std::shared_ptr<FollowUpScanner> follow_up_scanner_;
    std::optional<int64_t> next_snapshot_id_;
};
}  // namespace paimon
