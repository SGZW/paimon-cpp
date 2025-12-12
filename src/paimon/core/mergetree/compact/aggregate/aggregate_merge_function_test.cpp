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

#include "paimon/core/mergetree/compact/aggregate/aggregate_merge_function.h"

#include <map>
#include <variant>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_min_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_primary_key_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_sum_agg.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(AggregateMergeFunctionTest, TestGetAggFuncName) {
    {
        // test with specified agg
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                                                   {"fields.f0.aggregate-function", "min"}}));
        ASSERT_OK_AND_ASSIGN(std::string str_agg, AggregateMergeFunction::GetAggFuncName(
                                                      "f0", /*primary_keys=*/{"f1"}, options));
        ASSERT_EQ(FieldMinAgg::NAME, str_agg);
    }
    {
        // test with default agg
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
        ASSERT_OK_AND_ASSIGN(std::string str_agg, AggregateMergeFunction::GetAggFuncName(
                                                      "f0", /*primary_keys=*/{"f1"}, options));
        ASSERT_EQ(FieldSumAgg::NAME, str_agg);
    }
    {
        // test no agg configuration
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({}));
        ASSERT_OK_AND_ASSIGN(std::string str_agg, AggregateMergeFunction::GetAggFuncName(
                                                      "f0", /*primary_keys=*/{"f1"}, options));
        ASSERT_EQ(FieldLastNonNullValueAgg::NAME, str_agg);
    }
    {
        // test primary key
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
        ASSERT_OK_AND_ASSIGN(std::string str_agg, AggregateMergeFunction::GetAggFuncName(
                                                      "f0", /*primary_keys=*/{"f0"}, options));
        ASSERT_EQ(FieldPrimaryKeyAgg::NAME, str_agg);
    }
    {
        // test sequence fields
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "f0"}}));
        ASSERT_OK_AND_ASSIGN(std::string str_agg, AggregateMergeFunction::GetAggFuncName(
                                                      "f0", /*primary_keys=*/{"f1"}, options));
        ASSERT_EQ(FieldLastValueAgg::NAME, str_agg);
    }
}
TEST(AggregateMergeFunctionTest, TestSimple) {
    arrow::FieldVector fields = {arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto value_schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<AggregateMergeFunction> merge_func,
        AggregateMergeFunction::Create(value_schema, /*primary_keys=*/{"k0"}, core_options));

    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/0, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Delete(), /*sequence_number=*/0, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));
    ASSERT_OK(merge_func->Add(std::move(kv1)));
    auto result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0,
                      /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/2);

    merge_func->Reset();
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    ASSERT_OK(merge_func->Add(std::move(kv3)));
    result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/0,
                       /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({10, -100}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/2);
}

TEST(AggregateMergeFunctionTest, TestIgnoreRetract) {
    arrow::FieldVector fields = {arrow::field("k0", arrow::int32()),
                                 arrow::field("v0", arrow::int32())};
    auto value_schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                                               {"fields.v0.ignore-retract", "true"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<AggregateMergeFunction> merge_func,
        AggregateMergeFunction::Create(value_schema, /*primary_keys=*/{"k0"}, core_options));

    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/0, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValue kv3(RowKind::Delete(), /*sequence_number=*/1, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 300}, pool.get()));
    ASSERT_OK(merge_func->Add(std::move(kv1)));
    auto result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0,
                      /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 100}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/2);

    merge_func->Reset();
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    ASSERT_OK(merge_func->Add(std::move(kv3)));
    result_kv = std::move(merge_func->GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/1,
                       /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 200}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/2);
}

TEST(AggregateMergeFunctionTest, TestSequenceFields) {
    arrow::FieldVector fields = {
        arrow::field("k0", arrow::int32()), arrow::field("s0", arrow::int32()),
        arrow::field("s1", arrow::int32()), arrow::field("v0", arrow::int32())};
    auto value_schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions core_options,
                         CoreOptions::FromMap({{Options::SEQUENCE_FIELD, "s0,s1"},
                                               {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"}}));
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<AggregateMergeFunction> merge_func,
        AggregateMergeFunction::Create(value_schema, /*primary_keys=*/{"k0"}, core_options));
    auto pool = GetDefaultPool();
    // sequence: null, 2
    KeyValue kv1(
        RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
        /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({10, NullType(), 2, 200}, pool.get()));
    // sequence: 1, null
    KeyValue kv2(
        RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
        BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 1, NullType(), 100}, pool.get()));
    merge_func->Reset();
    ASSERT_OK(merge_func->Add(std::move(kv1)));
    ASSERT_OK(merge_func->Add(std::move(kv2)));
    KeyValue result_kv = std::move(merge_func->GetResult().value().value());
    // expect sequence: 1, null
    KeyValue expected(
        RowKind::Insert(), /*sequence_number=*/0, /*level=*/KeyValue::UNKNOWN_LEVEL, /*key=*/
        BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
        /*value=*/BinaryRowGenerator::GenerateRowPtr({10, 1, NullType(), 300}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/4);
}

}  // namespace paimon::test
