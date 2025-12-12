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
#include <string>
#include <vector>

#include "paimon/predicate/predicate.h"
#include "paimon/visibility.h"

namespace paimon {
class CompoundFunction;
class Function;

/// Non-leaf node in a `Predicate` tree. Its evaluation result depends on the results of its
/// children.
class PAIMON_EXPORT CompoundPredicate : virtual public Predicate {
 public:
    const Function& GetFunction() const override;

    const std::vector<std::shared_ptr<Predicate>>& Children() const {
        return children_;
    }

    std::shared_ptr<Predicate> Negate() const override;
    std::string ToString() const override;

    bool operator==(const Predicate& other) const override;

 protected:
    CompoundPredicate(const CompoundFunction& compound_function,
                      const std::vector<std::shared_ptr<Predicate>>& children);

    const CompoundFunction& compound_function_;
    std::vector<std::shared_ptr<Predicate>> children_;
};
}  // namespace paimon
