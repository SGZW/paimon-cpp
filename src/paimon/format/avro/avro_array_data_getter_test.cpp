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

#include "paimon/format/avro/avro_array_data_getter.h"

#include <limits>
#include <string>

#include "avro/GenericDatum.hh"
#include "avro/Schema.hh"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::avro::test {

TEST(AvroArrayDataGetterTest, TestBasic) {
    {
        ::avro::GenericArray bool_array(::avro::ArraySchema(::avro::BoolSchema()).root());
        bool_array.value().push_back(true);
        bool_array.value().push_back(false);
        AvroArrayDataGetter getter(bool_array, GetDefaultPool());
        ASSERT_EQ(true, getter.GetBoolean(0));
        ASSERT_EQ(false, getter.GetBoolean(1));
    }
    {
        ::avro::GenericArray char_array(::avro::ArraySchema(::avro::IntSchema()).root());
        char_array.value().push_back(std::numeric_limits<char>::max());
        char_array.value().push_back(std::numeric_limits<char>::min());
        AvroArrayDataGetter getter(char_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<char>::max(), getter.GetByte(0));
        ASSERT_EQ(std::numeric_limits<char>::min(), getter.GetByte(1));
    }
    {
        ::avro::GenericArray short_array(::avro::ArraySchema(::avro::IntSchema()).root());
        short_array.value().push_back(std::numeric_limits<int16_t>::max());
        short_array.value().push_back(std::numeric_limits<int16_t>::min());
        AvroArrayDataGetter getter(short_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<int16_t>::max(), getter.GetShort(0));
        ASSERT_EQ(std::numeric_limits<int16_t>::min(), getter.GetShort(1));
    }
    {
        ::avro::GenericArray int_array(::avro::ArraySchema(::avro::IntSchema()).root());
        int_array.value().push_back(std::numeric_limits<int32_t>::max());
        int_array.value().push_back(std::numeric_limits<int32_t>::min());
        AvroArrayDataGetter getter(int_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<int32_t>::max(), getter.GetInt(0));
        ASSERT_EQ(std::numeric_limits<int32_t>::min(), getter.GetInt(1));
    }
    {
        ::avro::GenericArray long_array(::avro::ArraySchema(::avro::LongSchema()).root());
        long_array.value().push_back(std::numeric_limits<int64_t>::max());
        long_array.value().push_back(std::numeric_limits<int64_t>::min());
        AvroArrayDataGetter getter(long_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<int64_t>::max(), getter.GetLong(0));
        ASSERT_EQ(std::numeric_limits<int64_t>::min(), getter.GetLong(1));
    }
    {
        ::avro::GenericArray float_array(::avro::ArraySchema(::avro::FloatSchema()).root());
        float_array.value().push_back(std::numeric_limits<float>::max());
        float_array.value().push_back(std::numeric_limits<float>::min());
        AvroArrayDataGetter getter(float_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<float>::max(), getter.GetFloat(0));
        ASSERT_EQ(std::numeric_limits<float>::min(), getter.GetFloat(1));
    }
    {
        ::avro::GenericArray double_array(::avro::ArraySchema(::avro::DoubleSchema()).root());
        double_array.value().push_back(std::numeric_limits<double>::max());
        double_array.value().push_back(std::numeric_limits<double>::min());
        AvroArrayDataGetter getter(double_array, GetDefaultPool());
        ASSERT_EQ(std::numeric_limits<double>::max(), getter.GetDouble(0));
        ASSERT_EQ(std::numeric_limits<double>::min(), getter.GetDouble(1));
    }
    {
        ::avro::GenericArray string_array(::avro::ArraySchema(::avro::StringSchema()).root());
        string_array.value().push_back(std::string("apple"));
        string_array.value().push_back(std::string("banana"));
        AvroArrayDataGetter getter(string_array, GetDefaultPool());
        ASSERT_EQ("apple", getter.GetString(0).ToString());
        ASSERT_EQ("banana", getter.GetString(1).ToString());
    }
}

}  // namespace paimon::avro::test
