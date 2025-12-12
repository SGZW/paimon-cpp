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

#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(_MSC_VER)
#pragma warning(disable : 4251)
#else
#pragma GCC diagnostic ignored "-Wattributes"
#endif

#ifdef PAIMON_STATIC
#define PAIMON_EXPORT
#elif defined(PAIMON_EXPORTING)
#define PAIMON_EXPORT __declspec(dllexport)
#else
#define PAIMON_EXPORT __declspec(dllimport)
#endif

#define PAIMON_NO_EXPORT
#define PAIMON_FORCE_INLINE __forceinline
#else  // Not Windows
#ifndef PAIMON_EXPORT
#define PAIMON_EXPORT __attribute__((visibility("default")))
#endif
#ifndef PAIMON_NO_EXPORT
#define PAIMON_NO_EXPORT __attribute__((visibility("hidden")))
#define PAIMON_FORCE_INLINE
#endif
#endif  // Non-Windows
