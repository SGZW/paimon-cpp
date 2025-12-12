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

#include "paimon/format/avro/avro_record_converter.h"

#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "avro/Compiler.hh"
#include "avro/GenericDatum.hh"
#include "avro/ValidSchema.hh"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {
class AvroRecordConverterTest : public testing::Test {
 public:
    std::shared_ptr<const ::avro::ValidSchema> CreateSimpleSchema() {
        std::string schema_str = R"({
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "is_active", "type": "boolean"}
        ]
    })";
        return std::make_shared<const ::avro::ValidSchema>(
            ::avro::compileJsonSchemaFromString(schema_str));
    }

    std::vector<::avro::GenericDatum> CreateTestDatums(int count) {
        auto valid_schema = CreateSimpleSchema();
        std::vector<::avro::GenericDatum> datums;
        for (int i = 0; i < count; ++i) {
            ::avro::GenericRecord record(valid_schema->root());
            record.setFieldAt(0, ::avro::GenericDatum(i + 1));
            record.setFieldAt(1, ::avro::GenericDatum("user_" + std::to_string(i + 1)));
            record.setFieldAt(2, ::avro::GenericDatum(i % 2 == 0));

            ::avro::GenericDatum datum(*valid_schema);
            auto& value = datum.value<::avro::GenericRecord>();
            value = std::move(record);
            datums.emplace_back(std::move(datum));
        }
        return datums;
    }

    std::shared_ptr<arrow::Schema> CreateExpectedArrowSchema() {
        auto id_field = arrow::field("id", arrow::int32());
        auto name_field = arrow::field("name", arrow::utf8());
        auto active_field = arrow::field("is_active", arrow::boolean());
        return arrow::schema({id_field, name_field, active_field});
    }
};

TEST_F(AvroRecordConverterTest, NextBatchConvertsSimpleDataCorrectly) {
    auto arrow_schema = CreateExpectedArrowSchema();
    auto arrow_type = arrow::struct_({arrow_schema->fields()});
    ASSERT_OK_AND_ASSIGN(auto converter, AvroRecordConverter::Create(arrow_type, GetDefaultPool()));

    auto avro_datums = CreateTestDatums(3);
    ASSERT_OK_AND_ASSIGN(auto batch, converter->NextBatch(avro_datums));
    auto [c_array, c_schema] = std::move(batch);
    ASSERT_EQ(c_array->length, 3);
    auto array = arrow::ImportArray(c_array.get(), c_schema.get()).ValueOr(nullptr);
    ASSERT_TRUE(array);
    std::string data_str = R"([
[1, "user_1", true],
[2, "user_2", false],
[3, "user_3", true]
])";
    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow_type, data_str).ValueOr(nullptr);
    ASSERT_TRUE(expected_array);
    ASSERT_TRUE(expected_array->Equals(array));
}

}  // namespace paimon::avro::test
