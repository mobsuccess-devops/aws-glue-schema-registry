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
import com.amazonaws.services.schemaregistry.serializers.json.JsonValidator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.DecimalFormat
import org.everit.json.schema.ArraySchema
import org.everit.json.schema.BooleanSchema
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.ObjectSchema
import org.everit.json.schema.StringSchema
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.Collections
import org.everit.json.schema.Schema as JsonSchema

class FromConnectTest {
    private lateinit var connectSchemaToJsonSchemaConverter: ConnectSchemaToJsonSchemaConverter
    private lateinit var connectValueToJsonNodeConverter: ConnectValueToJsonNodeConverter

    @BeforeEach
    fun setUp() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name),
            )
        connectSchemaToJsonSchemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)
        connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(jsonSchemaDataConfig)
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testSchemaAndValueArgumentsProvider",
    )
    fun testFromConnect_schemaAndValue_asExpected(
        expectedJsonSchema: JsonSchema,
        connectSchema: Schema?,
        expectedJsonValue: JsonNode,
        connectValue: Any?,
    ) {
        val actualJsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema)

        assertEquals(expectedJsonSchema, actualJsonSchema)

        val actualJsonValue = connectValueToJsonNodeConverter.convertToJson(connectSchema, connectValue)

        if (!expectedJsonValue.isNull || !expectedJsonSchema.hasDefaultValue()) {
            assertEquals(expectedJsonValue, actualJsonValue)
        }

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
                OBJECT_MAPPER.readTree(actualJsonValue.toString()),
            )
        }
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testInvalidSchemaAndValueArgumentsProvider",
    )
    fun testFromConnect_invalidSchemaAndValue_throwsDataException(
        expectedJsonSchema: JsonSchema,
        connectSchema: Schema?,
        connectValue: Any?,
    ) {
        val actualJsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema)

        assertEquals(expectedJsonSchema, actualJsonSchema)

        assertThrows(DataException::class.java) {
            connectValueToJsonNodeConverter.convertToJson(connectSchema, connectValue)
        }
    }

    @Test
    fun testFromConnect_base64Decimal_asExpected() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.BASE64.name),
            )
        val schemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)
        val valueConverter = ConnectValueToJsonNodeConverter(jsonSchemaDataConfig)

        val actualJsonSchema = schemaConverter.fromConnectSchema(TestDataProvider.CONNECT_DECIMAL_SCHEMA)

        assertEquals(TestDataProvider.STRING_DECIMAL_SCHEMA, actualJsonSchema)

        val actualJsonValue =
            valueConverter.convertToJson(
                TestDataProvider.CONNECT_DECIMAL_SCHEMA,
                TestDataProvider.CONNECT_DECIMAL_VALUE,
            )

        assertEquals(TestDataProvider.BASE64_DECIMAL_JSON_NODE, actualJsonValue)

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
                OBJECT_MAPPER.readTree(actualJsonValue.toString()),
            )
        }

        val actualHighPrecisionDecimalJsonSchema =
            schemaConverter.fromConnectSchema(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA)

        assertEquals(TestDataProvider.STRING_HIGH_PRECISION_DECIMAL_SCHEMA, actualHighPrecisionDecimalJsonSchema)

        val actualHighPrecisionDecimalJsonValue =
            valueConverter.convertToJson(
                TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA,
                TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_VALUE,
            )

        assertEquals(TestDataProvider.BASE64_HIGH_PRECISION_DECIMAL_JSON_NODE, actualHighPrecisionDecimalJsonValue)

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualHighPrecisionDecimalJsonSchema.toString()),
                OBJECT_MAPPER.readTree(actualHighPrecisionDecimalJsonValue.toString()),
            )
        }
    }

    @Test
    fun testFromConnect_complexStruct_asExpected() {
        val connectSchema =
            SchemaBuilder
                .struct()
                .field("int8", SchemaBuilder.int8().defaultValue(2.toByte()).doc("int8 field").build())
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
                .build()
        val connectValue =
            Struct(connectSchema)
                .put("int8", 42.toByte())
                .put("int16", 42.toShort())
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".toByteArray())
                .put("array", listOf("a", "b", "c"))
                .put("map", Collections.singletonMap("field", 1))
                .put("mapNonStringKeys", Collections.singletonMap(1, 1))

        val complexMapElementSchema =
            ArraySchema
                .builder()
                .allItemSchema(
                    ObjectSchema
                        .builder()
                        .addPropertySchema(JsonSchemaConverterConstants.KEY_FIELD, TestDataProvider.INT_SCHEMA)
                        .addPropertySchema(JsonSchemaConverterConstants.VALUE_FIELD, TestDataProvider.INT_SCHEMA)
                        .build(),
                ).unprocessedProperties(
                    hashMapOf<String, Any>(
                        JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map",
                        JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 11,
                    ),
                ).build()

        // One field has some extra data set on it to ensure it gets passed through via the fields
        // config
        val byteSchemaWithDefault =
            NumberSchema
                .builder()
                .requiresInteger(true)
                .unprocessedProperties(
                    hashMapOf<String, Any>(
                        JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "int8",
                        JsonSchemaConverterConstants.CONNECT_DOC_PROP to "int8 field",
                        JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 0,
                    ),
                ).defaultValue(2)
                .build()

        val expectedJsonSchema =
            ObjectSchema
                .builder()
                .addPropertySchema("int8", byteSchemaWithDefault)
                .addPropertySchema(
                    "int16",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.SHORT_SCHEMA_BUILDER, 1),
                ).addPropertySchema(
                    "int32",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.INT_SCHEMA_BUILDER, 2),
                ).addPropertySchema(
                    "int64",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.LONG_SCHEMA_BUILDER, 3),
                ).addPropertySchema(
                    "float32",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.FLOAT_SCHEMA_BUILDER, 4),
                ).addPropertySchema(
                    "float64",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.DOUBLE_SCHEMA_BUILDER, 5),
                ).addPropertySchema("boolean", TestDataProvider.buildSchemaWithIndex(BooleanSchema.builder(), 6))
                .addPropertySchema("string", TestDataProvider.buildSchemaWithIndex(StringSchema.builder(), 7))
                .addPropertySchema(
                    "bytes",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.BYTES_SCHEMA_BUILDER, 8),
                ).addPropertySchema(
                    "array",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.ARRAY_SCHEMA_BUILDER, 9),
                ).addPropertySchema(
                    "map",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.MAP_SCHEMA_WITH_STRING_KEY_BUILDER, 10),
                ).addPropertySchema("mapNonStringKeys", complexMapElementSchema)
                .build()

        val actualJsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema)

        val convertedJsonNode = connectValueToJsonNodeConverter.convertToJson(connectSchema, connectValue)

        val array = JsonNodeFactory.instance.arrayNode()
        array.add("a").add("b").add("c")
        val expectedJsonNode =
            JSON_NODE_FACTORY
                .objectNode()
                .put("int8", 42)
                .put("int16", 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".toByteArray())
                .set<com.fasterxml.jackson.databind.node.ObjectNode>("array", array)

        expectedJsonNode.set<JsonNode>("map", JSON_NODE_FACTORY.objectNode().put("field", 1))

        expectedJsonNode.set<JsonNode>(
            "mapNonStringKeys",
            JSON_NODE_FACTORY.arrayNode().add(
                JSON_NODE_FACTORY
                    .objectNode()
                    .put(JsonSchemaConverterConstants.VALUE_FIELD, 1)
                    .put(JsonSchemaConverterConstants.KEY_FIELD, 1),
            ),
        )

        assertEquals(
            JsonSchemaConverter.canonicalize(expectedJsonSchema.toString()),
            JsonSchemaConverter.canonicalize(actualJsonSchema.toString()),
        )
        assertEquals(expectedJsonNode.toString(), convertedJsonNode.toString())

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
                OBJECT_MAPPER.readTree(convertedJsonNode.toString()),
            )
        }
    }

    @Test
    fun testFromConnectComplex_withDefaults_succeeds() {
        val dateDefVal = 100
        val timeDefVal = 1000 * 60 * 60 * 2
        val tsDefVal = 1000L * 60 * 60 * 24 * 365 + 100
        val dateDef = Date.toLogical(Date.SCHEMA, dateDefVal)
        val timeDef = Time.toLogical(Time.SCHEMA, timeDefVal)
        val tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal)
        val decimalDef = BigDecimal(BigInteger.valueOf(314159L), 5)

        val connectSchema =
            SchemaBuilder
                .struct()
                .field("int8", SchemaBuilder.int8().defaultValue(42.toByte()).doc("int8 field").build())
                .field("int16", SchemaBuilder.int16().defaultValue(42.toShort()).doc("int16 field").build())
                .field("int32", SchemaBuilder.int32().defaultValue(42).doc("int32 field").build())
                .field("int64", SchemaBuilder.int64().defaultValue(42L).doc("int64 field").build())
                .field("float32", SchemaBuilder.float32().defaultValue(42.42f).doc("float32 field").build())
                .field("float64", SchemaBuilder.float64().defaultValue(42.42).doc("float64 field").build())
                .field("boolean", SchemaBuilder.bool().defaultValue(true).doc("bool field").build())
                .field("string", SchemaBuilder.string().defaultValue("foo").doc("string field").build())
                .field(
                    "bytes",
                    SchemaBuilder
                        .bytes()
                        .defaultValue(ByteBuffer.wrap("foo".toByteArray()))
                        .doc("bytes field")
                        .build(),
                ).field(
                    "array",
                    SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(listOf("a", "b", "c")).build(),
                ).field(
                    "map",
                    SchemaBuilder
                        .map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                        .defaultValue(Collections.singletonMap("field", 1))
                        .build(),
                ).field("date", Date.builder().defaultValue(dateDef).doc("date field").build())
                .field("time", Time.builder().defaultValue(timeDef).doc("time field").build())
                .field("ts", Timestamp.builder().defaultValue(tsDef).doc("ts field").build())
                .field("decimal", Decimal.builder(5).defaultValue(decimalDef).doc("decimal field").build())
                .build()
        // leave the struct empty so that only defaults are used
        val connectValue =
            Struct(connectSchema)
                .put("int8", 42.toByte())
                .put("int16", 42.toShort())
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".toByteArray())
                .put("array", listOf("a", "b", "c"))
                .put("map", Collections.singletonMap("field", 1))
                .put("date", dateDef)
                .put("time", timeDef)
                .put("ts", tsDef)
                .put("decimal", decimalDef)

        val expectedJsonSchema =
            ObjectSchema
                .builder()
                .addPropertySchema("int8", numberWithDefault("int8", "int8 field", 0, 42, true))
                .addPropertySchema("int16", numberWithDefault("int16", "int16 field", 1, 42.toShort(), true))
                .addPropertySchema("int32", numberWithDefault("int32", "int32 field", 2, 42, true))
                .addPropertySchema("int64", numberWithDefault("int64", "int64 field", 3, 42L, true))
                .addPropertySchema("float32", numberWithDefault("float32", "float32 field", 4, 42.42f, false))
                .addPropertySchema("float64", numberWithDefault("float64", "float64 field", 5, 42.42, false))
                .addPropertySchema(
                    "boolean",
                    BooleanSchema
                        .builder()
                        .unprocessedProperties(
                            hashMapOf<String, Any>(
                                JsonSchemaConverterConstants.CONNECT_DOC_PROP to "bool field",
                                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 6,
                            ),
                        ).defaultValue(true)
                        .build(),
                ).addPropertySchema(
                    "string",
                    StringSchema
                        .builder()
                        .unprocessedProperties(
                            hashMapOf<String, Any>(
                                JsonSchemaConverterConstants.CONNECT_DOC_PROP to "string field",
                                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 7,
                            ),
                        ).defaultValue("foo")
                        .build(),
                ).addPropertySchema(
                    "bytes",
                    StringSchema
                        .builder()
                        .unprocessedProperties(
                            hashMapOf<String, Any>(
                                JsonSchemaConverterConstants.CONNECT_DOC_PROP to "bytes field",
                                JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 8,
                            ),
                        ).defaultValue(JSON_NODE_FACTORY.binaryNode(ByteBuffer.wrap("foo".toByteArray()).array()))
                        .build(),
                ).addPropertySchema(
                    "array",
                    ArraySchema
                        .builder()
                        .allItemSchema(TestDataProvider.STRING_SCHEMA)
                        .unprocessedProperties(
                            hashMapOf<String, Any>(JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 9),
                        ).defaultValue(listOf("a", "b", "c"))
                        .build(),
                ).addPropertySchema(
                    "map",
                    ObjectSchema
                        .builder()
                        .schemaOfAdditionalProperties(TestDataProvider.INT_SCHEMA)
                        .unprocessedProperties(
                            hashMapOf<String, Any>(
                                JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "map",
                                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 10,
                            ),
                        ).defaultValue(Collections.singletonMap("field", 1))
                        .build(),
                ).addPropertySchema(
                    "date",
                    logicalWithDefault(Date.LOGICAL_NAME, "date field", "int32", 11, JSON_NODE_FACTORY.numberNode(dateDefVal)),
                ).addPropertySchema(
                    "time",
                    logicalWithDefault(Time.LOGICAL_NAME, "time field", "int32", 12, JSON_NODE_FACTORY.numberNode(timeDefVal)),
                ).addPropertySchema(
                    "ts",
                    logicalWithDefault(Timestamp.LOGICAL_NAME, "ts field", "int64", 13, JSON_NODE_FACTORY.numberNode(tsDefVal)),
                ).addPropertySchema(
                    "decimal",
                    NumberSchema
                        .builder()
                        .unprocessedProperties(
                            hashMapOf<String, Any>(
                                JsonSchemaConverterConstants.CONNECT_TYPE_PROP to "bytes",
                                JsonSchemaConverterConstants.CONNECT_NAME_PROP to Decimal.LOGICAL_NAME,
                                JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                                JsonSchemaConverterConstants.CONNECT_DOC_PROP to "decimal field",
                                JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP to
                                    Collections.singletonMap("scale", "5"),
                                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to 14,
                            ),
                        ).defaultValue(JSON_NODE_FACTORY.numberNode(decimalDef))
                        .build(),
                ).build()

        val array = JsonNodeFactory.instance.arrayNode()
        array.add("a").add("b").add("c")
        val expectedJsonNode =
            JSON_NODE_FACTORY
                .objectNode()
                .put("int8", 42)
                .put("int16", 42)
                .put("int32", 42)
                .put("int64", 42L)
                .put("float32", 42.42f)
                .put("float64", 42.42)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", "foo".toByteArray())
                .set<com.fasterxml.jackson.databind.node.ObjectNode>("array", array)

        expectedJsonNode.set<JsonNode>("map", JSON_NODE_FACTORY.objectNode().put("field", 1))

        expectedJsonNode
            .put("date", dateDefVal)
            .put("time", timeDefVal)
            .put("ts", tsDefVal)
            .put("decimal", decimalDef)

        val actualJsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema)

        val convertedJsonNode = connectValueToJsonNodeConverter.convertToJson(connectSchema, connectValue)

        assertEquals(
            OBJECT_MAPPER.readTree(expectedJsonSchema.toString()),
            OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
        )

        assertEquals(expectedJsonNode.toString(), convertedJsonNode.toString())

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
                OBJECT_MAPPER.readTree(convertedJsonNode.toString()),
            )
        }
    }

    @Test
    fun testFromConnectStruct_withMetadata_succeeds() {
        val connectSchema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.test.TestSchema")
                .version(12)
                .doc("doc")
                .field("int32", Schema.INT32_SCHEMA)
                .build()
        val connectValue = Struct(connectSchema).put("int32", 42)

        val actualJsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema)
        val expectedJsonSchema =
            ObjectSchema
                .builder()
                .addPropertySchema(
                    "int32",
                    TestDataProvider.buildSchemaWithIndex(TestDataProvider.INT_SCHEMA_BUILDER, 0),
                ).unprocessedProperties(
                    hashMapOf<String, Any>(
                        JsonSchemaConverterConstants.CONNECT_NAME_PROP to
                            "com.amazonaws.services.schemaregistry.test.TestSchema",
                        JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 12,
                        JsonSchemaConverterConstants.CONNECT_DOC_PROP to "doc",
                    ),
                ).build()

        assertEquals(expectedJsonSchema, actualJsonSchema)

        val expectedJsonNode: JsonNode = JSON_NODE_FACTORY.objectNode().put("int32", 42)
        val convertedJsonNode = connectValueToJsonNodeConverter.convertToJson(connectSchema, connectValue)

        assertEquals(convertedJsonNode, expectedJsonNode)

        assertDoesNotThrow {
            JSON_VALIDATOR.validateDataWithSchema(
                OBJECT_MAPPER.readTree(actualJsonSchema.toString()),
                OBJECT_MAPPER.readTree(convertedJsonNode.toString()),
            )
        }
    }

    @Test
    fun testFromConnect_sameTypeFields_keepTheirOwnIndex() {
        val connectSchema =
            SchemaBuilder
                .struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .build()

        val jsonSchema = connectSchemaToJsonSchemaConverter.fromConnectSchema(connectSchema) as ObjectSchema

        assertEquals(0, connectIndexOf(jsonSchema, "a"))
        assertEquals(1, connectIndexOf(jsonSchema, "b"))
    }

    private fun connectIndexOf(
        objectSchema: ObjectSchema,
        fieldName: String,
    ): Int? = objectSchema.propertySchemas[fieldName]
        ?.unprocessedProperties
        ?.get(JsonSchemaConverterConstants.CONNECT_INDEX_PROP) as Int?

    @Test
    fun testSchemaCache_size_fromConnectConversion() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(Collections.singletonMap(JsonSchemaDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2))
        val schemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)

        val cache = schemaConverter.fromConnectSchemaCache
        assertEquals(0, cache.size())

        schemaConverter.fromConnectSchema(Schema.BOOLEAN_SCHEMA)
        assertEquals(1, cache.size())

        schemaConverter.fromConnectSchema(Schema.BOOLEAN_SCHEMA)
        assertEquals(1, cache.size())

        schemaConverter.fromConnectSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA)
        assertEquals(2, cache.size())

        // Should hit limit of cache
        schemaConverter.fromConnectSchema(Schema.STRING_SCHEMA)
        assertEquals(2, cache.size())
    }

    /** The Java original repeated this builder for every numeric field; the shape is identical. */
    private fun numberWithDefault(
        connectType: String,
        doc: String,
        index: Int,
        default: Any,
        requiresInteger: Boolean,
    ): JsonSchema = NumberSchema
        .builder()
        .requiresInteger(requiresInteger)
        .unprocessedProperties(
            hashMapOf<String, Any>(
                JsonSchemaConverterConstants.CONNECT_TYPE_PROP to connectType,
                JsonSchemaConverterConstants.CONNECT_DOC_PROP to doc,
                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to index,
            ),
        ).defaultValue(default)
        .build()

    private fun logicalWithDefault(
        logicalName: String,
        doc: String,
        connectType: String,
        index: Int,
        default: Any,
    ): JsonSchema = NumberSchema
        .builder()
        .requiresInteger(true)
        .unprocessedProperties(
            hashMapOf<String, Any>(
                JsonSchemaConverterConstants.CONNECT_NAME_PROP to logicalName,
                JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 1,
                JsonSchemaConverterConstants.CONNECT_DOC_PROP to doc,
                JsonSchemaConverterConstants.CONNECT_TYPE_PROP to connectType,
                JsonSchemaConverterConstants.CONNECT_INDEX_PROP to index,
            ),
        ).defaultValue(default)
        .build()

    companion object {
        private val JSON_NODE_FACTORY: JsonNodeFactory = TypeConverter.JSON_NODE_FACTORY
        private val OBJECT_MAPPER = ObjectMapper()
        private val JSON_VALIDATOR = JsonValidator()
    }
}
