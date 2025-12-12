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

#include "paimon/core/mergetree/compact/first_row_merge_function.h"

#include <memory>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(FirstRowMergeFunctionTest, TestSimple) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/1,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/2, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    FirstRowMergeFunction mfunc(/*ignore_delete=*/true);
    mfunc.Reset();
    ASSERT_EQ(std::nullopt, mfunc.GetResult().value());
    ASSERT_OK(mfunc.Add(std::move(kv1)));
    ASSERT_EQ(mfunc.contains_high_level_, false);
    ASSERT_OK(mfunc.Add(std::move(kv2)));
    KeyValue result_kv = std::move(mfunc.GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/1);
    ASSERT_EQ(mfunc.contains_high_level_, true);

    mfunc.Reset();
    ASSERT_EQ(mfunc.contains_high_level_, false);
    ASSERT_EQ(std::nullopt, mfunc.GetResult().value());
    ASSERT_OK(mfunc.Add(std::move(kv3)));
    result_kv = std::move(mfunc.GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/2, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/1);
    ASSERT_EQ(mfunc.contains_high_level_, true);
}

TEST(FirstRowMergeFunctionTest, TestIgnoreDelete) {
    auto pool = GetDefaultPool();
    KeyValue kv0(RowKind::Delete(), /*sequence_number=*/0, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({50}, pool.get()));

    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Delete(), /*sequence_number=*/1, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));

    FirstRowMergeFunction mfunc(/*ignore_delete=*/true);
    mfunc.Reset();
    ASSERT_EQ(std::nullopt, mfunc.GetResult().value());
    ASSERT_OK(mfunc.Add(std::move(kv0)));
    ASSERT_EQ(std::nullopt, mfunc.GetResult().value());
    ASSERT_EQ(mfunc.contains_high_level_, false);

    mfunc.Reset();
    ASSERT_OK(mfunc.Add(std::move(kv1)));
    ASSERT_EQ(mfunc.contains_high_level_, false);
    ASSERT_OK(mfunc.Add(std::move(kv2)));

    auto result_kv = std::move(mfunc.GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/1);
    ASSERT_EQ(mfunc.contains_high_level_, false);
}

TEST(FirstRowMergeFunctionTest, TestDeleteRecordWithoutSetIgnoreDelete) {
    auto pool = GetDefaultPool();
    KeyValue kv0(RowKind::Delete(), /*sequence_number=*/0, /*level=*/2,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({50}, pool.get()));
    FirstRowMergeFunction mfunc(/*ignore_delete=*/false);
    mfunc.Reset();
    ASSERT_EQ(std::nullopt, mfunc.GetResult().value());
    ASSERT_NOK(mfunc.Add(std::move(kv0)));
}
}  // namespace paimon::test
