/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverter
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.collect.Sets
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.everit.json.schema.ArraySchema
import org.everit.json.schema.BooleanSchema
import org.everit.json.schema.CombinedSchema
import org.everit.json.schema.EnumSchema
import org.everit.json.schema.NullSchema
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.ObjectSchema
import org.everit.json.schema.StringSchema
import org.junit.jupiter.params.provider.Arguments
import java.math.BigDecimal
import java.util.Collections
import java.util.stream.Stream
import org.everit.json.schema.Schema as JsonSchema

/**
 * Shared fixtures for the module: the JSON schemas, their Connect counterparts, and the
 * @MethodSource providers that pair them with values. The Java original exposed these as
 * package-private statics; internal is the Kotlin equivalent within the test source set.
 */
@Suppress("ktlint:standard:property-naming")
object TestDataProvider {
    internal val JSON_NODE_FACTORY: JsonNodeFactory = TypeConverter.JSON_NODE_FACTORY

    internal val NULL_NODE: NullNode = NullNode.getInstance()
    internal val NULL_SCHEMA: JsonSchema = NullSchema.INSTANCE
    internal val BOOLEAN_SCHEMA: JsonSchema = BooleanSchema.builder().build()
    internal val BYTE_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int8"))
    internal val BYTE_SCHEMA: JsonSchema = BYTE_SCHEMA_BUILDER.build()
    internal val SHORT_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int16"))
    internal val SHORT_SCHEMA: JsonSchema = SHORT_SCHEMA_BUILDER.build()
    internal val INT_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int32"))
    internal val INT_SCHEMA: JsonSchema = INT_SCHEMA_BUILDER.build()
    internal val LONG_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int64"))
    internal val LONG_SCHEMA: JsonSchema = LONG_SCHEMA_BUILDER.build()
    internal val FLOAT_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(false)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "float32"))
    internal val FLOAT_SCHEMA: JsonSchema = FLOAT_SCHEMA_BUILDER.build()
    internal val DOUBLE_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        NumberSchema
            .builder()
            .requiresInteger(false)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "float64"))
    internal val DOUBLE_SCHEMA: JsonSchema = DOUBLE_SCHEMA_BUILDER.build()
    internal val BYTES_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        StringSchema
            .builder()
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes"))
    internal val BYTES_SCHEMA: JsonSchema = BYTES_SCHEMA_BUILDER.build()
    internal val STRING_SCHEMA: JsonSchema = StringSchema.builder().build()
    internal val ENUM_SCHEMA: JsonSchema =
        EnumSchema
            .builder()
            .possibleValues(Sets.newHashSet<Any>("apple", "banana", "mango"))
            .build()
    internal val CONNECT_ENUM_SCHEMA: Schema =
        SchemaBuilder(Schema.Type.STRING)
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM, null)
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".apple", "apple")
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".banana", "banana")
            .parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".mango", "mango")
            .build()
    internal val MAP_SCHEMA_WITH_STRING_KEY_BUILDER: JsonSchema.Builder<*> =
        ObjectSchema
            .builder()
            .schemaOfAdditionalProperties(INT_SCHEMA)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map"))
    internal val MAP_SCHEMA_WITH_STRING_KEY: JsonSchema = MAP_SCHEMA_WITH_STRING_KEY_BUILDER.build()
    internal val CONNECT_MAP_SCHEMA_WITH_STRING_KEY: Schema =
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
    internal val CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY: Schema =
        SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build()
    internal val ARRAY_SCHEMA_BUILDER: JsonSchema.Builder<*> =
        ArraySchema.builder().allItemSchema(STRING_SCHEMA)
    internal val ARRAY_SCHEMA: JsonSchema = ARRAY_SCHEMA_BUILDER.build()
    internal val OPTIONAL_BOOLEAN_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(BOOLEAN_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT: JsonSchema =
        CombinedSchema.oneOf(listOf(BOOLEAN_SCHEMA, NULL_SCHEMA)).defaultValue(null).build()
    internal val CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT: Schema =
        SchemaBuilder.bool().optional().defaultValue(null).build()
    internal val OPTIONAL_BYTE_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(BYTE_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_SHORT_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(SHORT_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_INT_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(INT_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_LONG_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(LONG_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_FLOAT_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(FLOAT_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_DOUBLE_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(DOUBLE_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_BYTES_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(BYTES_SCHEMA, NULL_SCHEMA)).build()
    internal val OPTIONAL_STRING_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(STRING_SCHEMA, NULL_SCHEMA)).build()
    internal val MAP_SCHEMA_WITH_OPTIONAL_STRING_KEY: JsonSchema =
        ObjectSchema
            .builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, OPTIONAL_STRING_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build()
    internal val MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY: JsonSchema =
        ArraySchema
            .builder()
            .allItemSchema(MAP_SCHEMA_WITH_OPTIONAL_STRING_KEY)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map"))
            .build()
    internal val MAP_JSON_DATA_WITH_STRING_KEY: JsonNode =
        JSON_NODE_FACTORY
            .objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, "answer")
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42)
    internal val MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY: ArrayNode =
        JSON_NODE_FACTORY.arrayNode().add(MAP_JSON_DATA_WITH_STRING_KEY)
    internal val MAP_JSON_DATA_WITH_NULL_KEY: JsonNode =
        JSON_NODE_FACTORY
            .objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, null as String?)
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42)
    internal val MAP_JSON_DATA_AS_ARRAY_WITH_NULL_KEY: ArrayNode =
        JSON_NODE_FACTORY.arrayNode().add(MAP_JSON_DATA_WITH_NULL_KEY)
    internal val MAP_SCHEMA_WITH_INTEGER_KEY: JsonSchema =
        ObjectSchema
            .builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, INT_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build()
    internal val MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER: JsonSchema.Builder<*> =
        ArraySchema
            .builder()
            .allItemSchema(MAP_SCHEMA_WITH_INTEGER_KEY)
            .unprocessedProperties(hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map"))
    internal val MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY: JsonSchema = MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER.build()

    internal val MAP_JSON_DATA_WITH_INTEGER_KEY: JsonNode =
        JSON_NODE_FACTORY
            .objectNode()
            .put(JsonSchemaConverterConstants.KEY_FIELD, 42)
            .put(JsonSchemaConverterConstants.VALUE_FIELD, 42)
    internal val MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY: ArrayNode =
        JsonNodeFactory.instance.arrayNode().add(MAP_JSON_DATA_WITH_INTEGER_KEY)
    internal val CONNECT_NAMED_MAP_SCHEMA: Schema =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA).name("foo.bar").build()
    internal val MAP_NAMED_SCHEMA_WITH_OPTIONAL_STRING_KEY: JsonSchema =
        ObjectSchema
            .builder()
            .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, OPTIONAL_STRING_SCHEMA)
            .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, INT_SCHEMA)
            .build()
    internal val MAP_NAMED_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY: JsonSchema =
        ArraySchema
            .builder()
            .allItemSchema(MAP_NAMED_SCHEMA_WITH_OPTIONAL_STRING_KEY)
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map",
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to "foo.bar",
                ),
            ).build()

    internal val CONNECT_STRUCT_SCHEMA: Schema =
        SchemaBuilder.struct().field("int32", Schema.INT32_SCHEMA).build()
    internal val CONNECT_STRUCT_STRING_SCHEMA: Schema =
        SchemaBuilder.struct().field("int32", Schema.STRING_SCHEMA).build()
    internal val CONNECT_STRUCT_VALUE: Struct = Struct(CONNECT_STRUCT_SCHEMA).put("int32", 42)
    internal val JSON_STRUCT_SCHEMA: JsonSchema =
        ObjectSchema.builder().addPropertySchema("int32", buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0)).build()
    internal val JSON_STRUCT_DATA: JsonNode = JsonNodeFactory.instance.objectNode().put("int32", 42)
    internal val CONNECT_OPTIONAL_STRUCT_SCHEMA: Schema =
        SchemaBuilder.struct().optional().field("int32", Schema.INT32_SCHEMA).build()
    internal val JSON_STRUCT_OPTIONAL_SCHEMA: JsonSchema =
        CombinedSchema.oneOf(listOf(NullSchema.INSTANCE, JSON_STRUCT_SCHEMA)).build()
    internal val CONNECT_STRUCT_OPTIONAL_VALUE: Struct = Struct(CONNECT_OPTIONAL_STRUCT_SCHEMA).put("int32", 42)

    internal val CONNECT_DECIMAL_SCHEMA: Schema = Decimal.builder(2).build()
    internal val NUMBER_DECIMAL_SCHEMA: JsonSchema =
        NumberSchema
            .builder()
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Decimal.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP to Collections.singletonMap("scale", "2"),
                ),
            ).build()
    internal val STRING_DECIMAL_SCHEMA: JsonSchema =
        StringSchema
            .builder()
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Decimal.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP to Collections.singletonMap("scale", "2"),
                ),
            ).build()
    internal val CONNECT_DECIMAL_VALUE: BigDecimal = BigDecimal.valueOf(156, 2)
    internal val NUMERIC_DECIMAL_JSON_NODE: JsonNode = JSON_NODE_FACTORY.numberNode(CONNECT_DECIMAL_VALUE)
    internal val BASE64_DECIMAL_JSON_NODE: JsonNode = JSON_NODE_FACTORY.binaryNode(byteArrayOf(0, -100))
    internal val CONNECT_DECIMAL_VALUE_TRAILING_ZEROES: BigDecimal = BigDecimal("156.00")
    internal val NUMERIC_DECIMAL_JSON_NODE_TRAILING_ZEROES: JsonNode =
        JSON_NODE_FACTORY.numberNode(CONNECT_DECIMAL_VALUE_TRAILING_ZEROES)

    internal val CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA: Schema = Decimal.builder(17).build()
    internal val NUMBER_HIGH_PRECISION_DECIMAL_SCHEMA: JsonSchema =
        NumberSchema
            .builder()
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Decimal.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP to Collections.singletonMap("scale", "17"),
                ),
            ).build()
    internal val STRING_HIGH_PRECISION_DECIMAL_SCHEMA: JsonSchema =
        StringSchema
            .builder()
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Decimal.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP to Collections.singletonMap("scale", "17"),
                ),
            ).build()
    internal val CONNECT_HIGH_PRECISION_DECIMAL_VALUE: BigDecimal =
        BigDecimal("79228162514264337593543950335.23456789123456789")
    internal val NUMERIC_HIGH_PRECISION_DECIMAL_JSON_NODE: JsonNode =
        DecimalNode.valueOf(CONNECT_HIGH_PRECISION_DECIMAL_VALUE)
    internal val BASE64_HIGH_PRECISION_DECIMAL_JSON_NODE: JsonNode =
        JSON_NODE_FACTORY.binaryNode(CONNECT_HIGH_PRECISION_DECIMAL_VALUE.unscaledValue().toByteArray())

    internal val CONNECT_DATE_SCHEMA: Schema = Date.builder().build()
    internal val DATE_SCHEMA: JsonSchema =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Date.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int32",
                ),
            ).build()
    internal val CONNECT_DATE_VALUE: java.util.Date = Date.toLogical(CONNECT_DATE_SCHEMA, 1620233761)
    internal val DATE_JSON_NODE: JsonNode = JSON_NODE_FACTORY.numberNode(1620233761)

    internal val CONNECT_TIME_SCHEMA: Schema = Time.builder().build()
    internal val TIME_SCHEMA: JsonSchema =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Time.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int32",
                ),
            ).build()
    internal val CONNECT_TIME_VALUE: java.util.Date = Time.toLogical(CONNECT_TIME_SCHEMA, 86400000)
    internal val TIME_JSON_NODE: JsonNode = JSON_NODE_FACTORY.numberNode(86400000)

    internal val CONNECT_TIMESTAMP_SCHEMA: Schema = Timestamp.builder().build()
    internal val TIMESTAMP_SCHEMA: JsonSchema =
        NumberSchema
            .builder()
            .requiresInteger(true)
            .unprocessedProperties(
                hashMapOf<String, Any>(
                    JsonSchemaConverterConstants.CONNECT_NAME_PROP to Timestamp.LOGICAL_NAME,
                    JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                    JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int64",
                ),
            ).build()
    internal val CONNECT_TIMESTAMP_VALUE: java.util.Date = Timestamp.toLogical(CONNECT_TIMESTAMP_SCHEMA, 1620233761102L)
    internal val TIMESTAMP_JSON_NODE: JsonNode = JSON_NODE_FACTORY.numberNode(1620233761102L)

    internal val CONNECT_UNION_SCHEMA: Schema =
        SchemaBuilder
            .struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.INT32_SCHEMA)
            .field("field2", Schema.STRING_SCHEMA)
            .build()
    internal val CONNECT_UNION_VALUE_1: Struct = Struct(CONNECT_UNION_SCHEMA).put("field1", 42)
    internal val CONNECT_UNION_VALUE_2: Struct = Struct(CONNECT_UNION_SCHEMA).put("field2", "answer")
    internal val EXPECTED_JSON_UNION_SCHEMA: JsonSchema =
        CombinedSchema
            .oneOf(
                listOf(buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0), buildSchemaWithIndex(StringSchema.builder(), 1)),
            ).build()
    internal val CONNECT_UNION_SCHEMA_BYTE_STRING: Schema =
        SchemaBuilder
            .struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.STRING_SCHEMA)
            .field("field2", Schema.INT8_SCHEMA)
            .build()
    internal val CONNECT_UNION_BYTE_STRING_VALUE_1: Struct =
        Struct(CONNECT_UNION_SCHEMA_BYTE_STRING).put("field1", "answer")
    internal val CONNECT_UNION_BYTE_STRING_VALUE_2: Struct =
        Struct(CONNECT_UNION_SCHEMA_BYTE_STRING).put("field2", 42.toByte())
    internal val EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA: JsonSchema =
        CombinedSchema
            .oneOf(
                listOf(buildSchemaWithIndex(StringSchema.builder(), 0), buildSchemaWithIndex(BYTE_SCHEMA_BUILDER, 1)),
            ).build()
    internal val CONNECT_UNION_INT_BYTE_SCHEMA: Schema =
        SchemaBuilder
            .struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.INT32_SCHEMA)
            .field("field2", Schema.INT8_SCHEMA)
            .build()
    internal val CONNECT_UNION_INT_BYTE_VALUE_1: Struct = Struct(CONNECT_UNION_INT_BYTE_SCHEMA).put("field1", 42)
    internal val CONNECT_UNION_INT_BYTE_VALUE_2: Struct =
        Struct(CONNECT_UNION_INT_BYTE_SCHEMA).put("field2", 10.toByte())
    internal val EXPECTED_JSON_INT_BYTE_SCHEMA: JsonSchema =
        CombinedSchema
            .oneOf(
                listOf(buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0), buildSchemaWithIndex(BYTE_SCHEMA_BUILDER, 1)),
            ).build()

    internal val CONNECT_UNION_SCHEMA_MIXED: Schema =
        SchemaBuilder
            .struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", Schema.FLOAT32_SCHEMA)
            .field("field2", Schema.BOOLEAN_SCHEMA)
            .build()
    internal val CONNECT_UNION_MIXED_VALUE_1: Struct = Struct(CONNECT_UNION_SCHEMA_MIXED).put("field1", 17.17f)
    internal val CONNECT_UNION_MIXED_VALUE_2: Struct = Struct(CONNECT_UNION_SCHEMA_MIXED).put("field2", true)
    internal val EXPECTED_JSON_MIXED_UNION_SCHEMA: JsonSchema =
        CombinedSchema
            .oneOf(
                listOf(buildSchemaWithIndex(FLOAT_SCHEMA_BUILDER, 0), buildSchemaWithIndex(BooleanSchema.builder(), 1)),
            ).build()
    internal val CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES: Schema =
        SchemaBuilder
            .struct()
            .name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)
            .field("field1", CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY)
            .field("field2", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build()
    internal val CONNECT_UNION_NON_PRIMITIVE_VALUE_1: Struct =
        Struct(CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES).put("field1", Collections.singletonMap(42, 42))
    internal val CONNECT_UNION_NON_PRIMITIVE_VALUE_2: Struct =
        Struct(CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES).put("field2", listOf("a", "b", "c"))
    internal val EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES: JsonSchema =
        CombinedSchema
            .oneOf(
                listOf(
                    buildSchemaWithIndex(MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY_BUILDER, 0),
                    buildSchemaWithIndex(ARRAY_SCHEMA_BUILDER, 1),
                ),
            ).build()
    internal val CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS: Schema =
        SchemaBuilder
            .struct()
            .field("int32", Schema.INT32_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .build()
    internal val CONNECT_STRUCT_WITH_MISSING_FIELDS: Struct =
        Struct(CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS).put("int32", 42).put("boolean", true)
    internal val JSON_SCHEMA_FOR_MISSING_FIELDS: JsonSchema =
        ObjectSchema
            .builder()
            .addPropertySchema("int32", buildSchemaWithIndex(INT_SCHEMA_BUILDER, 0))
            .addPropertySchema("boolean", buildSchemaWithIndex(BooleanSchema.builder(), 1))
            .addPropertySchema("string", buildSchemaWithIndex(StringSchema.builder(), 2))
            .build()
    internal val JSON_NODE_WITH_MISSING_FIELDS: ObjectNode =
        JSON_NODE_FACTORY.objectNode().put("int32", 42).put("boolean", true)
    internal val ARRAY_NODE: ArrayNode = JsonNodeFactory.instance.arrayNode().add("a").add("b").add("c")

    internal val BOOLEAN_SCHEMA_WITH_DEFAULT: JsonSchema = BooleanSchema.builder().defaultValue(true).build()

    internal val CONNECT_BOOLEAN_SCHEMA_WITH_DEFAULT: Schema = SchemaBuilder.bool().defaultValue(true).build()

    internal fun buildSchemaWithIndex(
        schemaBuilder: JsonSchema.Builder<*>,
        connectIndex: Int?,
    ): JsonSchema {
        val unprocessedProperties = schemaBuilder.unprocessedProperties
        if (connectIndex != null) {
            unprocessedProperties[JsonSchemaConverterConstants.CONNECT_INDEX_PROP] = connectIndex
        }
        return schemaBuilder.unprocessedProperties(unprocessedProperties).build()
    }

    @JvmStatic
    fun testSchemaAndValueArgumentsProvider(): Stream<Arguments> = Stream.of(
        Arguments.of(NULL_SCHEMA, null, NULL_NODE, null),
        Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, BooleanNode.getTrue(), true),
        Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, BooleanNode.getFalse(), false),
        Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, NULL_NODE, null),
        Arguments.of(BOOLEAN_SCHEMA_WITH_DEFAULT, CONNECT_BOOLEAN_SCHEMA_WITH_DEFAULT, NULL_NODE, null),
        Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, BooleanNode.getTrue(), true),
        Arguments.of(OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA, BooleanNode.getFalse(), false),
        Arguments.of(
            OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            NULL_NODE,
            null,
        ),
        Arguments.of(
            OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            BooleanNode.getTrue(),
            true,
        ),
        Arguments.of(
            OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            CONNECT_OPTIONAL_BOOLEAN_SCHEMA_WITH_NULL_DEFAULT,
            BooleanNode.getFalse(),
            false,
        ),
        Arguments.of(BYTE_SCHEMA, Schema.INT8_SCHEMA, JSON_NODE_FACTORY.numberNode(42.toByte()), 42.toByte()),
        Arguments.of(OPTIONAL_BYTE_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA, NULL_NODE, null),
        Arguments.of(SHORT_SCHEMA, Schema.INT16_SCHEMA, JSON_NODE_FACTORY.numberNode(42.toShort()), 42.toShort()),
        Arguments.of(OPTIONAL_SHORT_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA, NULL_NODE, null),
        Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, JSON_NODE_FACTORY.numberNode(42), 42),
        Arguments.of(OPTIONAL_INT_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA, NULL_NODE, null),
        Arguments.of(LONG_SCHEMA, Schema.INT64_SCHEMA, JSON_NODE_FACTORY.numberNode(42L), 42L),
        Arguments.of(OPTIONAL_LONG_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA, NULL_NODE, null),
        Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, JSON_NODE_FACTORY.numberNode(42.42f), 42.42f),
        Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, JSON_NODE_FACTORY.numberNode(1.7f), 1.7f),
        Arguments.of(OPTIONAL_FLOAT_SCHEMA, Schema.OPTIONAL_FLOAT32_SCHEMA, NULL_NODE, null),
        Arguments.of(DOUBLE_SCHEMA, Schema.FLOAT64_SCHEMA, JSON_NODE_FACTORY.numberNode(42.42), 42.42),
        Arguments.of(DOUBLE_SCHEMA, Schema.FLOAT64_SCHEMA, JSON_NODE_FACTORY.numberNode(1.7), 1.7),
        Arguments.of(OPTIONAL_DOUBLE_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA, NULL_NODE, null),
        Arguments.of(
            BYTES_SCHEMA,
            Schema.BYTES_SCHEMA,
            JSON_NODE_FACTORY.binaryNode("answer".toByteArray()),
            "answer".toByteArray(),
        ),
        Arguments.of(OPTIONAL_BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA, NULL_NODE, null),
        Arguments.of(STRING_SCHEMA, Schema.STRING_SCHEMA, JSON_NODE_FACTORY.textNode("answer"), "answer"),
        Arguments.of(OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, NULL_NODE, null),
        Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("apple"), "apple"),
        Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("mango"), "mango"),
        Arguments.of(ENUM_SCHEMA, CONNECT_ENUM_SCHEMA, JSON_NODE_FACTORY.textNode("banana"), "banana"),
        Arguments.of(
            MAP_SCHEMA_WITH_STRING_KEY,
            CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
            JSON_NODE_FACTORY.objectNode().put("answer", 42),
            Collections.singletonMap("answer", 42),
        ),
        Arguments.of(
            MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY,
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
            MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY,
            Collections.singletonMap("answer", 42),
        ),
        Arguments.of(
            MAP_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY,
            SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
            MAP_JSON_DATA_AS_ARRAY_WITH_NULL_KEY,
            Collections.singletonMap(null, 42),
        ),
        Arguments.of(
            MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY,
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
            MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY,
            Collections.singletonMap(42, 42),
        ),
        Arguments.of(
            MAP_NAMED_ARRAY_SCHEMA_WITH_OPTIONAL_STRING_KEY,
            CONNECT_NAMED_MAP_SCHEMA,
            MAP_JSON_DATA_AS_ARRAY_WITH_STRING_KEY,
            Collections.singletonMap("answer", 42),
        ),
        Arguments.of(JSON_STRUCT_SCHEMA, CONNECT_STRUCT_SCHEMA, JSON_STRUCT_DATA, CONNECT_STRUCT_VALUE),
        Arguments.of(
            JSON_STRUCT_OPTIONAL_SCHEMA,
            CONNECT_OPTIONAL_STRUCT_SCHEMA,
            JSON_STRUCT_DATA,
            CONNECT_STRUCT_OPTIONAL_VALUE,
        ),
        Arguments.of(
            ARRAY_SCHEMA,
            SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
            ARRAY_NODE,
            listOf("a", "b", "c"),
        ),
        Arguments.of(NUMBER_DECIMAL_SCHEMA, CONNECT_DECIMAL_SCHEMA, NUMERIC_DECIMAL_JSON_NODE, CONNECT_DECIMAL_VALUE),
        Arguments.of(
            NUMBER_HIGH_PRECISION_DECIMAL_SCHEMA,
            CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA,
            NUMERIC_HIGH_PRECISION_DECIMAL_JSON_NODE,
            CONNECT_HIGH_PRECISION_DECIMAL_VALUE,
        ),
        Arguments.of(
            NUMBER_DECIMAL_SCHEMA,
            CONNECT_DECIMAL_SCHEMA,
            NUMERIC_DECIMAL_JSON_NODE_TRAILING_ZEROES,
            CONNECT_DECIMAL_VALUE_TRAILING_ZEROES,
        ),
        Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, DATE_JSON_NODE, CONNECT_DATE_VALUE),
        Arguments.of(TIME_SCHEMA, CONNECT_TIME_SCHEMA, TIME_JSON_NODE, CONNECT_TIME_VALUE),
        Arguments.of(TIMESTAMP_SCHEMA, CONNECT_TIMESTAMP_SCHEMA, TIMESTAMP_JSON_NODE, CONNECT_TIMESTAMP_VALUE),
        Arguments.of(
            EXPECTED_JSON_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA,
            JSON_NODE_FACTORY.numberNode(42),
            CONNECT_UNION_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA,
            JSON_NODE_FACTORY.textNode("answer"),
            CONNECT_UNION_VALUE_2,
        ),
        Arguments.of(
            EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_BYTE_STRING,
            JSON_NODE_FACTORY.textNode("answer"),
            CONNECT_UNION_BYTE_STRING_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_BYTE_STRING,
            JSON_NODE_FACTORY.numberNode(42.toByte()),
            CONNECT_UNION_BYTE_STRING_VALUE_2,
        ),
        Arguments.of(
            EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_BYTE_STRING,
            JSON_NODE_FACTORY.textNode("answer"),
            CONNECT_UNION_BYTE_STRING_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_BYTE_STRING_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_BYTE_STRING,
            JSON_NODE_FACTORY.numberNode(42.toByte()),
            CONNECT_UNION_BYTE_STRING_VALUE_2,
        ),
        Arguments.of(
            EXPECTED_JSON_MIXED_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_MIXED,
            JSON_NODE_FACTORY.numberNode(17.17f),
            CONNECT_UNION_MIXED_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_MIXED_UNION_SCHEMA,
            CONNECT_UNION_SCHEMA_MIXED,
            JSON_NODE_FACTORY.booleanNode(true),
            CONNECT_UNION_MIXED_VALUE_2,
        ),
        Arguments.of(
            EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES,
            CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
            MAP_JSON_DATA_AS_ARRAY_WITH_INTEGER_KEY,
            CONNECT_UNION_NON_PRIMITIVE_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES,
            CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
            ARRAY_NODE,
            CONNECT_UNION_NON_PRIMITIVE_VALUE_2,
        ),
        Arguments.of(
            JSON_SCHEMA_FOR_MISSING_FIELDS,
            CONNECT_STRUCT_SCHEMA_FOR_MISSING_FIELDS,
            JSON_NODE_WITH_MISSING_FIELDS,
            CONNECT_STRUCT_WITH_MISSING_FIELDS,
        ),
    )

    @JvmStatic
    fun testInvalidSchemaAndValueArgumentsProvider(): Stream<Arguments> = Stream.of(
        Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, null),
        Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, 42),
        Arguments.of(BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, listOf(42)),
        Arguments.of(BYTE_SCHEMA, Schema.INT8_SCHEMA, 42),
        Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, 42L),
        Arguments.of(INT_SCHEMA, Schema.INT32_SCHEMA, 42.42f),
        Arguments.of(FLOAT_SCHEMA, Schema.FLOAT32_SCHEMA, 42.42),
        Arguments.of(OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA, 123),
        Arguments.of(
            MAP_SCHEMA_WITH_STRING_KEY,
            CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
            Collections.singletonMap(true, 42),
        ),
        Arguments.of(
            MAP_SCHEMA_WITH_STRING_KEY,
            CONNECT_MAP_SCHEMA_WITH_STRING_KEY,
            Collections.singletonMap("answer", 42L),
        ),
        Arguments.of(
            MAP_ARRAY_SCHEMA_WITH_INTEGER_KEY,
            CONNECT_MAP_SCHEMA_WITH_INTEGER_KEY,
            Collections.singletonMap("answer", 42),
        ),
        Arguments.of(ARRAY_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA).build(), listOf(1, 2, 3)),
        Arguments.of(BYTES_SCHEMA, Schema.BYTES_SCHEMA, "answer"),
        Arguments.of(
            JSON_STRUCT_SCHEMA,
            CONNECT_STRUCT_SCHEMA,
            Struct(CONNECT_STRUCT_STRING_SCHEMA).put("int32", "42"),
        ),
        Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, CONNECT_TIMESTAMP_VALUE),
        Arguments.of(DATE_SCHEMA, CONNECT_DATE_SCHEMA, 86400000),
        Arguments.of(NUMBER_DECIMAL_SCHEMA, CONNECT_DECIMAL_SCHEMA, 156.00),
        Arguments.of(
            EXPECTED_JSON_INT_BYTE_SCHEMA,
            CONNECT_UNION_INT_BYTE_SCHEMA,
            JSON_NODE_FACTORY.numberNode(42),
            CONNECT_UNION_INT_BYTE_VALUE_1,
        ),
        Arguments.of(
            EXPECTED_JSON_INT_BYTE_SCHEMA,
            CONNECT_UNION_INT_BYTE_SCHEMA,
            JSON_NODE_FACTORY.numberNode(10.toByte()),
            CONNECT_UNION_INT_BYTE_VALUE_2,
        ),
        Arguments.of(
            EXPECTED_JSON_UNION_SCHEMA_NON_PRIMITIVES,
            CONNECT_UNION_SCHEMA_OF_NON_PRIMITIVES,
            Collections.singletonMap(true, 42),
        ),
    )
}
