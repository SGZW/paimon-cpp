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

#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace paimon {
template <typename T>
struct is_vector : std::false_type {};

template <typename T>
struct is_vector<std::vector<T>> : std::true_type {};

template <typename T>
struct is_map : std::false_type {};

template <typename K, typename V>
struct is_map<std::map<K, V>> : std::true_type {};

template <typename T>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <typename T>
struct is_raw_pointer : std::is_pointer<T> {};

template <typename T>
struct is_shared_ptr : std::false_type {};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

template <typename T>
struct is_unique_ptr : std::false_type {};

template <typename T>
struct is_unique_ptr<std::unique_ptr<T>> : std::true_type {};

template <typename T, typename Deleter>
struct is_unique_ptr<std::unique_ptr<T, Deleter>> : std::true_type {};

template <typename T>
struct is_weak_ptr : std::false_type {};

template <typename T>
struct is_weak_ptr<std::weak_ptr<T>> : std::true_type {};

// Combined checker for raw pointer or smart pointers
template <bool... B>
struct disjunction_impl;

template <bool... B>
struct disjunction_impl<true, B...> : std::true_type {};

template <bool... B>
struct disjunction_impl<false, B...> : disjunction_impl<B...> {};

template <>
struct disjunction_impl<> : std::false_type {};

template <typename... Traits>
using disjunction = disjunction_impl<Traits::value...>;

template <typename T>
struct is_pointer
    : disjunction<is_raw_pointer<T>, is_shared_ptr<T>, is_unique_ptr<T>, is_weak_ptr<T>> {};

template <typename T>
struct value_type_traits {
    using type = T;
};
template <typename T, typename Deleter>
struct value_type_traits<std::unique_ptr<T, Deleter>> {
    using type = T*;
};
template <typename T>
using value_type_traits_t = typename value_type_traits<T>::type;

static_assert(is_vector<std::vector<int32_t>>::value);
static_assert(is_map<std::map<int64_t, int32_t>>::value);
static_assert(is_optional<std::optional<int32_t>>::value);
static_assert(!is_optional<std::vector<int32_t>>::value);
static_assert(is_pointer<std::shared_ptr<int32_t>>::value);
static_assert(is_pointer<std::unique_ptr<int32_t>>::value);
static_assert(is_pointer<std::weak_ptr<int32_t>>::value);
static_assert(is_pointer<int32_t*>::value);
static_assert(!is_pointer<int32_t>::value);
}  // namespace paimon
