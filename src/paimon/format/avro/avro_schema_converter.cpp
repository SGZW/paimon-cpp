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

#include "paimon/format/avro/avro_schema_converter.h"

#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

#include "arrow/util/checked_cast.h"
#include "avro/CustomAttributes.hh"
#include "avro/LogicalType.hh"
#include "avro/Node.hh"
#include "avro/Schema.hh"
#include "avro/Types.hh"
#include "avro/ValidSchema.hh"
#include "fmt/format.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/macros.h"
#include "paimon/status.h"

namespace paimon::avro {

/// Returns schema with nullable true.
::avro::Schema AvroSchemaConverter::NullableSchema(const ::avro::Schema& schema) {
    assert(schema.type() != ::avro::AVRO_UNION);
    ::avro::UnionSchema union_schema;
    union_schema.addType(::avro::NullSchema());
    union_schema.addType(schema);
    return union_schema;
}

Result<bool> AvroSchemaConverter::CheckUnionType(const ::avro::NodePtr& avro_node) {
    auto type = avro_node->type();
    if (type == ::avro::AVRO_UNION) {
        if (avro_node->leaves() != 2) {
            return Status::Invalid("not support avro union leaves not 2");
        }
        auto node = avro_node->leafAt(0);
        if (node->type() != ::avro::AVRO_NULL) {
            return Status::Invalid("not support avro union first leaf is not avro null");
        }
        return true;
    }
    return false;
}

Result<std::shared_ptr<arrow::DataType>> AvroSchemaConverter::AvroSchemaToArrowDataType(
    const ::avro::ValidSchema& avro_schema) {
    ::avro::NodePtr root = avro_schema.root();
    PAIMON_ASSIGN_OR_RAISE(bool is_union, CheckUnionType(root));
    if (is_union) {
        root = root->leafAt(1);
    }
    if (PAIMON_UNLIKELY(root->type() != ::avro::AVRO_RECORD)) {
        return Status::Invalid("Avro schema root node is not a record type");
    }
    bool nullable = false;
    return GetArrowType(root, &nullable);
}

Result<std::shared_ptr<arrow::Field>> AvroSchemaConverter::GetArrowField(
    const std::string& name, const ::avro::NodePtr& avro_node) {
    bool nullable = false;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> arrow_type,
                           GetArrowType(avro_node, &nullable));
    return arrow::field(name, std::move(arrow_type), nullable);
}

Result<std::shared_ptr<arrow::DataType>> AvroSchemaConverter::GetArrowType(
    const ::avro::NodePtr& avro_node, bool* nullable) {
    PAIMON_ASSIGN_OR_RAISE(bool is_union, CheckUnionType(avro_node));
    if (is_union) {
        *nullable = true;
        return GetArrowType(avro_node->leafAt(1), nullable);
    }
    auto type = avro_node->type();
    auto logical_type = avro_node->logicalType();
    switch (logical_type.type()) {
        case ::avro::LogicalType::Type::NONE:
            break;
        case ::avro::LogicalType::Type::DATE:
            if (type != ::avro::AVRO_INT) {
                return Status::TypeError("invalid avro date stored as ", toString(type));
            }
            return arrow::date32();
        case ::avro::LogicalType::Type::DECIMAL:
            if (type != ::avro::AVRO_BYTES) {
                return Status::TypeError("invalid avro decimal stored as ", toString(type));
            }
            return arrow::decimal128(logical_type.precision(), logical_type.scale());
        case ::avro::LogicalType::Type::TIMESTAMP_MILLIS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::MILLI);
        }
        case ::avro::LogicalType::Type::TIMESTAMP_MICROS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        }
        case ::avro::LogicalType::Type::TIMESTAMP_NANOS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            return arrow::timestamp(arrow::TimeUnit::NANO);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MILLIS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_MICROS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
        }
        case ::avro::LogicalType::Type::LOCAL_TIMESTAMP_NANOS: {
            if (type != ::avro::AVRO_LONG) {
                return Status::TypeError("invalid avro timestamp stored as ", toString(type));
            }
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return arrow::timestamp(arrow::TimeUnit::NANO, timezone);
        }
        default:
            return Status::NotImplemented("not support logical type ",
                                          std::to_string(logical_type.type()));
    }

    size_t subtype_count = avro_node->leaves();
    switch (type) {
        case ::avro::AVRO_BOOL: {
            return arrow::boolean();
        }
        case ::avro::AVRO_INT: {
            return arrow::int32();
        }
        case ::avro::AVRO_LONG: {
            return arrow::int64();
        }
        case ::avro::AVRO_FLOAT: {
            return arrow::float32();
        }
        case ::avro::AVRO_DOUBLE: {
            return arrow::float64();
        }
        case ::avro::AVRO_STRING: {
            return arrow::utf8();
        }
        case ::avro::AVRO_BYTES: {
            return arrow::binary();
        }
        case ::avro::AVRO_ARRAY: {
            if (subtype_count != 1) {
                return Status::TypeError("Invalid Avro List type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> child_field,
                                   GetArrowField("item", avro_node->leafAt(0)));
            return arrow::list(std::move(child_field));
        }
        case ::avro::AVRO_MAP: {
            if (subtype_count != 2) {
                return Status::TypeError("Invalid Avro Map type");
            }
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> key_field,
                                   GetArrowField("key", avro_node->leafAt(0)));
            key_field = key_field->WithNullable(false);
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> value_field,
                                   GetArrowField("value", avro_node->leafAt(1)));
            return std::make_shared<arrow::MapType>(std::move(key_field), std::move(value_field));
        }
        case ::avro::AVRO_RECORD: {
            arrow::FieldVector fields(subtype_count);
            for (size_t child = 0; child < subtype_count; ++child) {
                const auto& name = avro_node->nameAt(child);
                PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> child_field,
                                       GetArrowField(name, avro_node->leafAt(child)));
                fields[child] = std::move(child_field);
            }
            return arrow::struct_(std::move(fields));
        }
        default:
            return Status::TypeError("Unknown Avro type kind: ", toString(type));
    }
}

Result<::avro::Schema> AvroSchemaConverter::ArrowTypeToAvroSchema(
    const std::shared_ptr<arrow::Field>& field) {
    bool nullable = field->nullable();
    auto arrow_type = field->type();
    switch (arrow_type->id()) {
        case arrow::Type::BOOL:
            return nullable ? NullableSchema(::avro::BoolSchema()) : ::avro::BoolSchema();
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
            return nullable ? NullableSchema(::avro::IntSchema()) : ::avro::IntSchema();
        case arrow::Type::INT64:
            return nullable ? NullableSchema(::avro::LongSchema()) : ::avro::LongSchema();
        case arrow::Type::FLOAT:
            return nullable ? NullableSchema(::avro::FloatSchema()) : ::avro::FloatSchema();
        case arrow::Type::DOUBLE:
            return nullable ? NullableSchema(::avro::DoubleSchema()) : ::avro::DoubleSchema();
        case arrow::Type::STRING:
            return nullable ? NullableSchema(::avro::StringSchema()) : ::avro::StringSchema();
        case arrow::Type::BINARY:
            return nullable ? NullableSchema(::avro::BytesSchema()) : ::avro::BytesSchema();
        case arrow::Type::type::DATE32: {
            ::avro::IntSchema date_schema;
            ::avro::LogicalType date_type = ::avro::LogicalType(::avro::LogicalType::DATE);
            date_schema.root()->setLogicalType(date_type);
            return nullable ? NullableSchema(date_schema) : date_schema;
        }
        case arrow::Type::type::TIMESTAMP: {
            // TODO(jinli.zjw): support convert with multiple precision & timezone
            const auto& arrow_timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(arrow_type);
            if (!arrow_timestamp_type->timezone().empty()) {
                return Status::Invalid("Unsupported TimestampType with timezone");
            }
            if (arrow_timestamp_type->unit() != arrow::TimeUnit::type::NANO) {
                return Status::Invalid("Only supported TimestampType with nano time unit");
            }
            // NOTE: Java Avro only support TIMESTAMP_MILLIS && TIMESTAMP_MICROS
            ::avro::LogicalType timestamp_type =
                ::avro::LogicalType(::avro::LogicalType::TIMESTAMP_MICROS);
            ::avro::LongSchema timestamp_schema;
            timestamp_schema.root()->setLogicalType(timestamp_type);
            return nullable ? NullableSchema(timestamp_schema) : timestamp_schema;
        }
        case arrow::Type::type::DECIMAL128: {
            const auto& arrow_decimal_type =
                arrow::internal::checked_pointer_cast<arrow::Decimal128Type>(arrow_type);
            ::avro::BytesSchema decimal_schema;
            ::avro::LogicalType decimal_type = ::avro::LogicalType(::avro::LogicalType::DECIMAL);
            decimal_type.setPrecision(arrow_decimal_type->precision());
            decimal_type.setScale(arrow_decimal_type->scale());
            decimal_schema.root()->setLogicalType(decimal_type);
            return nullable ? NullableSchema(decimal_schema) : decimal_schema;
        }
        default:
            return Status::Invalid(fmt::format("Not support arrow type '{}' convert to avro",
                                               field->type()->ToString()));
    }
}

Result<::avro::ValidSchema> AvroSchemaConverter::ArrowSchemaToAvroSchema(
    const std::shared_ptr<arrow::Schema>& arrow_schema) {
    ::avro::RecordSchema record_schema("record");
    for (const auto& field : arrow_schema->fields()) {
        PAIMON_ASSIGN_OR_RAISE(::avro::Schema schema, ArrowTypeToAvroSchema(field));
        if (schema.type() == ::avro::Type::AVRO_UNION) {
            ::avro::CustomAttributes attrs;
            attrs.addAttribute("default", "null", /*addQuotes=*/false);
            record_schema.addField(field->name(), schema, attrs);
        } else {
            record_schema.addField(field->name(), schema);
        }
    }
    return ::avro::ValidSchema(record_schema);
}

}  // namespace paimon::avro
