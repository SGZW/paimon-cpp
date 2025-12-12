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

#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"

#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/key_value_checker.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ReducerMergeFunctionWrapperTest, TestSimple) {
    auto pool = GetDefaultPool();
    KeyValue kv1(RowKind::Insert(), /*sequence_number=*/0, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({100}, pool.get()));
    KeyValue kv2(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0,
                 /*key=*/BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValue kv3(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                 BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                 /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));

    auto mfunc = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/true);
    ReducerMergeFunctionWrapper func_wrapper(std::move(mfunc));

    func_wrapper.Reset();
    ASSERT_EQ(std::nullopt, func_wrapper.GetResult().value());
    ASSERT_OK(func_wrapper.Add(std::move(kv1)));
    ASSERT_OK(func_wrapper.Add(std::move(kv2)));
    auto result_kv = std::move(func_wrapper.GetResult().value().value());
    KeyValue expected(RowKind::Insert(), /*sequence_number=*/1, /*level=*/0, /*key=*/
                      BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                      /*value=*/BinaryRowGenerator::GenerateRowPtr({200}, pool.get()));
    KeyValueChecker::CheckResult(expected, result_kv, /*key_arity=*/1, /*value_arity=*/1);

    func_wrapper.Reset();
    ASSERT_EQ(std::nullopt, func_wrapper.GetResult().value());
    ASSERT_OK(func_wrapper.Add(std::move(kv3)));
    result_kv = std::move(func_wrapper.GetResult().value().value());
    KeyValue expected2(RowKind::Insert(), /*sequence_number=*/2, /*level=*/0, /*key=*/
                       BinaryRowGenerator::GenerateRowPtr({10}, pool.get()),
                       /*value=*/BinaryRowGenerator::GenerateRowPtr({300}, pool.get()));
    KeyValueChecker::CheckResult(expected2, result_kv, /*key_arity=*/1, /*value_arity=*/1);
}

}  // namespace paimon::test
