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

#include "paimon/catalog/catalog.h"

#include "gtest/gtest.h"
#include "paimon/defs.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(CatalogTest, Create) {
    std::map<std::string, std::string> options;
    options[Options::FILE_SYSTEM] = "local";
    options[Options::FILE_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create("path", options));
}

}  // namespace paimon::test
