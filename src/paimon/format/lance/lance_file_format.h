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
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/format/file_format.h"
#include "paimon/format/lance/lance_reader_builder.h"
#include "paimon/format/lance/lance_stats_extractor.h"
#include "paimon/format/lance/lance_writer_builder.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace paimon {
class FormatStatsExtractor;
}  // namespace paimon
struct ArrowSchema;

namespace paimon::lance {

class LanceFileFormat : public FileFormat {
 public:
    explicit LanceFileFormat(const std::map<std::string, std::string>& options)
        : identifier_("lance"), options_(options) {}

    const std::string& Identifier() const override {
        return identifier_;
    }

    Result<std::unique_ptr<ReaderBuilder>> CreateReaderBuilder(int32_t batch_size) const override {
        return std::make_unique<LanceReaderBuilder>(options_, batch_size);
    }

    Result<std::unique_ptr<WriterBuilder>> CreateWriterBuilder(::ArrowSchema* schema,
                                                               int32_t batch_size) const override {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> typed_schema,
                                          arrow::ImportSchema(schema));
        return std::make_unique<LanceWriterBuilder>(typed_schema, options_);
    }

    Result<std::unique_ptr<FormatStatsExtractor>> CreateStatsExtractor(
        ::ArrowSchema* schema) const override {
        ArrowSchemaRelease(schema);
        return std::make_unique<LanceStatsExtractor>(options_);
    }

 private:
    std::string identifier_;
    std::map<std::string, std::string> options_;
};
}  // namespace paimon::lance
