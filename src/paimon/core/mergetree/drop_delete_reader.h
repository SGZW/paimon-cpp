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
#include <optional>
#include <utility>

#include "paimon/common/types/row_kind.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"

namespace paimon {
class Metrics;

/// A `RecordReader` which drops `KeyValue` that does not meet `RowKind#isAdd` from
/// the wrapped reader.
class DropDeleteReader : public SortMergeReader {
 public:
    explicit DropDeleteReader(std::unique_ptr<SortMergeReader>&& reader)
        : reader_(std::move(reader)) {}

    class Iterator : public SortMergeReader::Iterator {
     public:
        explicit Iterator(std::unique_ptr<SortMergeReader::Iterator>&& iterator)
            : iterator_(std::move(iterator)) {}
        Result<bool> HasNext() override {
            while (true) {
                PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator_->HasNext());
                if (!has_next) {
                    return false;
                }
                result_ = std::move(iterator_->Next());
                if (result_.value().value_kind->IsAdd()) {
                    break;
                }
            }
            return true;
        }
        KeyValue&& Next() override {
            return std::move(result_).value();
        }

     private:
        std::optional<KeyValue> result_;
        std::unique_ptr<SortMergeReader::Iterator> iterator_;
    };

    Result<std::unique_ptr<SortMergeReader::Iterator>> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<SortMergeReader::Iterator> iter,
                               reader_->NextBatch());
        if (iter == nullptr) {
            return iter;
        }
        return std::make_unique<Iterator>(std::move(iter));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    std::unique_ptr<SortMergeReader> reader_;
};
}  // namespace paimon
