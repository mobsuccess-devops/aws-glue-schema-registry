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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.connect.data.ConnectSchema
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
import org.everit.json.schema.CombinedSchema
import org.everit.json.schema.NullSchema
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.ObjectSchema
import org.everit.json.schema.StringSchema
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Collections
import org.everit.json.schema.Schema as JsonSchema

class ToConnectTest {
    private lateinit var jsonNodeToConnectValueConverter: JsonNodeToConnectValueConverter
    private lateinit var jsonSchemaToConnectSchemaConverter: JsonSchemaToConnectSchemaConverter

    @BeforeEach
    fun setUp() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name),
            )
        jsonNodeToConnectValueConverter = JsonNodeToConnectValueConverter(jsonSchemaDataConfig)
        jsonSchemaToConnectSchemaConverter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig)
    }

    @Test
    fun testConverter_NonSourceConverterGenerated_JSONSchema() {
        val jsonSchemaObject =
            JSONObject(
                """
                {
                    "${'$'}id": "https://example.com/weather-report.schema.json",
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "title": "WeatherReport",
                    "type": "object",
                    "properties": {
                        "location": {
                            "type": "object",
                            "properties": {
                                "city": {
                                    "type": "string",
                                    "description": "Name of the city where the weather is being reported."
                                },
                                "state": {
                                    "type": "string",
                                    "description": "Name of the state where the weather is being reported."
                                }
                            },
                            "additionalProperties": false,
                            "required": [
                                "city",
                                "state"
                            ]
                        },
                        "temperature": {
                            "type": "integer",
                            "description": "Temperature in Farenheit."
                        },
                        "timestamp": {
                            "description": "Timestamp in epoch format at which the weather was noted.",
                            "type": "integer"
                        }
                    },
                    "additionalProperties": true,
                    "required": [
                        "location",
                        "temperature",
                        "timestamp"
                    ]
                }
                """.trimIndent(),
            )

        val jsonSubject =
            JSONObject(
                """
                {
                    "location": {
                        "city": "Phoenix",
                        "state": "Arizona"
                    },
                    "temperature": 115,
                    "windSpeed": 50,
                    "timestamp": 1627335205
                }
                """.trimIndent(),
            )

        val jsonSchema = SchemaLoader.load(jsonSchemaObject)
        assertDoesNotThrow { jsonSchema.validate(jsonSubject) }

        val objectMapper = ObjectMapper()
        val jsonValue = objectMapper.readTree(jsonSubject.toString())

        val actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(actualConnectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(actualConnectSchema, actualConnectValue) }
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testSchemaAndValueArgumentsProvider",
    )
    fun testToConnect_schemaAndValue_asExpected(
        jsonSchema: JsonSchema,
        connectSchema: Schema?,
        jsonValue: JsonNode,
        expectedConnectValue: Any?,
    ) {
        val actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        if (expectedConnectValue != null &&
            expectedConnectValue.javaClass.isArray &&
            Schema.Type.BYTES == connectSchema!!.type()
        ) {
            assertArrayEquals(expectedConnectValue as ByteArray, actualConnectValue as ByteArray)
        } else if (!jsonValue.isNull || !jsonSchema.hasDefaultValue()) {
            assertEquals(expectedConnectValue, actualConnectValue)
        }

        val actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        assertEquals(connectSchema, actualConnectSchema)
    }

    @Test
    fun testToConnect_base64Decimal_asExpected() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(
                Collections.singletonMap(JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.BASE64.name),
            )
        val valueConverter = JsonNodeToConnectValueConverter(jsonSchemaDataConfig)

        val actualConnectValue =
            valueConverter.toConnectValue(
                TestDataProvider.CONNECT_DECIMAL_SCHEMA,
                TestDataProvider.BASE64_DECIMAL_JSON_NODE,
            )

        assertEquals(TestDataProvider.CONNECT_DECIMAL_VALUE, actualConnectValue)

        var actualConnectSchema =
            jsonSchemaToConnectSchemaConverter.toConnectSchema(TestDataProvider.STRING_DECIMAL_SCHEMA)

        assertEquals(TestDataProvider.CONNECT_DECIMAL_SCHEMA, actualConnectSchema)

        ConnectSchema.validateValue(TestDataProvider.CONNECT_DECIMAL_SCHEMA, actualConnectValue)

        val highPrecisionActualConnectValue =
            valueConverter.toConnectValue(
                TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA,
                TestDataProvider.BASE64_HIGH_PRECISION_DECIMAL_JSON_NODE,
            )

        assertEquals(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_VALUE, highPrecisionActualConnectValue)

        actualConnectSchema =
            jsonSchemaToConnectSchemaConverter.toConnectSchema(
                TestDataProvider.STRING_HIGH_PRECISION_DECIMAL_SCHEMA,
            )

        assertEquals(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA, actualConnectSchema)

        ConnectSchema.validateValue(TestDataProvider.CONNECT_HIGH_PRECISION_DECIMAL_SCHEMA, actualConnectValue)
    }

    @Test
    fun testToConnect_complexStruct_asExpected() {
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
        val byteSchemaWithDefault = numberWithDefault("int8", "int8 field", 0, 2.toByte(), true)

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
                .set<ObjectNode>("array", array)

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

        val actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode)

        assertEquals(connectValue, actualConnectValue)

        val actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema)

        assertEquals(connectSchema, actualConnectSchema)

        ConnectSchema.validateValue(actualConnectSchema, actualConnectValue)
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
                .field("bytes", SchemaBuilder.bytes().defaultValue("foo".toByteArray()).doc("bytes field").build())
                .field(
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
                .addPropertySchema("int8", numberWithDefault("int8", "int8 field", 0, 42.toByte(), true))
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
                        ).defaultValue("foo".toByteArray())
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
                ).addPropertySchema("date", logicalWithDefault(Date.LOGICAL_NAME, "date field", "int32", 11, dateDef))
                .addPropertySchema("time", logicalWithDefault(Time.LOGICAL_NAME, "time field", "int32", 12, timeDef))
                .addPropertySchema("ts", logicalWithDefault(Timestamp.LOGICAL_NAME, "ts field", "int64", 13, tsDef))
                .addPropertySchema(
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
                        ).defaultValue(decimalDef)
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
                .set<ObjectNode>("array", array)

        expectedJsonNode.set<JsonNode>("map", JSON_NODE_FACTORY.objectNode().put("field", 1))

        expectedJsonNode
            .put("date", dateDefVal)
            .put("time", timeDefVal)
            .put("ts", tsDefVal)
            .put("decimal", decimalDef)

        val actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode)

        assertEquals(connectValue, actualConnectValue)

        val actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema)

        assertEquals(connectSchema, actualConnectSchema)

        ConnectSchema.validateValue(connectSchema, actualConnectValue)
    }

    @Test
    fun testToConnectStruct_withMetadata_succeeds() {
        val connectSchema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.test.TestSchema")
                .version(12)
                .doc("doc")
                .field("int32", Schema.INT32_SCHEMA)
                .build()
        val connectValue = Struct(connectSchema).put("int32", 42)

        val expectedJsonSchema =
            ObjectSchema
                .builder()
                .addPropertySchema("int32", TestDataProvider.INT_SCHEMA)
                .unprocessedProperties(
                    hashMapOf<String, Any>(
                        JsonSchemaConverterConstants.CONNECT_NAME_PROP to
                            "com.amazonaws.services.schemaregistry.test.TestSchema",
                        JsonSchemaConverterConstants.CONNECT_VERSION_PROP to 12,
                        JsonSchemaConverterConstants.CONNECT_DOC_PROP to "doc",
                    ),
                ).build()

        val expectedJsonNode: JsonNode = JSON_NODE_FACTORY.objectNode().put("int32", 42)

        val actualConnectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, expectedJsonNode)

        assertEquals(connectValue, actualConnectValue)

        val actualConnectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(expectedJsonSchema)

        assertEquals(connectSchema, actualConnectSchema)

        ConnectSchema.validateValue(connectSchema, actualConnectValue)
    }

    @Test
    fun testToConnect_sameTypeRequiredAndNullableFields_requiredFieldConvertedFirst() {
        val connectSchema = toConnectStructOfStrings(requiredFieldName = "a", nullableFieldName = "b")

        assertFalse(connectSchema.field("a").schema().isOptional)
        assertTrue(connectSchema.field("b").schema().isOptional)

        val connectValue = Struct(connectSchema).put("a", "present")

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
    }

    @Test
    fun testToConnect_sameTypeRequiredAndNullableFields_nullableFieldConvertedFirst() {
        val connectSchema = toConnectStructOfStrings(requiredFieldName = "b", nullableFieldName = "a")

        assertTrue(connectSchema.field("a").schema().isOptional)
        assertFalse(connectSchema.field("b").schema().isOptional)

        val connectValue = Struct(connectSchema).put("b", "present")

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
    }

    private fun toConnectStructOfStrings(
        requiredFieldName: String,
        nullableFieldName: String,
    ): Schema {
        val nullableStringSchema =
            CombinedSchema
                .oneOf(listOf(NullSchema.builder().build(), StringSchema.builder().build()))
                .build()
        val jsonSchema =
            ObjectSchema
                .builder()
                .addPropertySchema(requiredFieldName, StringSchema.builder().build())
                .addPropertySchema(nullableFieldName, nullableStringSchema)
                .addRequiredProperty(requiredFieldName)
                .build()

        return jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!
    }

    @Test
    fun testSchemaCache_size_toConnectConversion() {
        val jsonSchemaDataConfig =
            JsonSchemaDataConfig(Collections.singletonMap(JsonSchemaDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 4))
        val schemaConverter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig)

        val cache = schemaConverter.toConnectSchemaCache
        assertEquals(0, cache.size())

        schemaConverter.toConnectSchema(BooleanSchema.builder().build())
        assertEquals(1, cache.size())

        schemaConverter.toConnectSchema(BooleanSchema.builder().build())
        assertEquals(1, cache.size())

        schemaConverter.toConnectSchema(TestDataProvider.INT_SCHEMA)
        assertEquals(2, cache.size())

        schemaConverter.toConnectSchema(TestDataProvider.LONG_SCHEMA)
        assertEquals(3, cache.size())

        schemaConverter.toConnectSchema(TestDataProvider.FLOAT_SCHEMA)
        assertEquals(4, cache.size())

        // Should hit limit of cache
        schemaConverter.toConnectSchema(StringSchema.builder().build())
        assertEquals(4, cache.size())
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

    @Test
    fun testToConnect_nullableTypeArray_isAnOptionalFieldOfTheRealType() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "null" ] """))

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val field = connectSchema!!.field("value")
        assertEquals(Schema.Type.STRING, field.schema().type())
        assertTrue(field.schema().isOptional)
    }

    @Test
    fun testToConnect_nullableTypeArray_readsAValue() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "null" ] """))
        val jsonValue = ObjectMapper().readTree("""{ "value": "Cristina Hermann" }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
        assertEquals("Cristina Hermann", (connectValue as Struct).get("value"))
    }

    @Test
    fun testToConnect_nullableTypeArray_readsANull() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "null" ] """))
        val jsonValue = ObjectMapper().readTree("""{ "value": null }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
    }

    @Test
    fun testToConnect_nullableAnyOf_isAnOptionalFieldOfTheRealType() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "value": { "anyOf": [ { "type": "string" }, { "type": "null" } ] }
                    },
                    "additionalProperties": false
                }
                """.trimIndent(),
            )

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val field = connectSchema!!.field("value")
        assertEquals(Schema.Type.STRING, field.schema().type())
        assertTrue(field.schema().isOptional)
    }

    @Test
    fun testToConnect_threeWayNullableUnion_isAnOptionalUnionOfTheRealTypes() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "integer", "null" ] """))

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val field = connectSchema!!.field("value")
        assertEquals(Schema.Type.STRUCT, field.schema().type())
        assertTrue(field.schema().isOptional)
        assertEquals(2, field.schema().fields().size)
    }

    @Test
    fun testToConnect_threeWayNullableUnion_readsANull() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "integer", "null" ] """))
        val jsonValue = ObjectMapper().readTree("""{ "value": null }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
    }

    @Test
    fun testToConnect_threeWayNullableUnion_readsANonNullValue() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "integer", "null" ] """))
        val jsonValue = ObjectMapper().readTree("""{ "value": "Cristina Hermann" }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
    }

    @Test
    fun testToConnect_threeWayNullableUnion_branchesAreOptional() {
        val jsonSchema = loadSchema(nullableSchema(""" [ "string", "integer", "null" ] """))

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val branches = connectSchema!!.field("value").schema().fields()
        assertTrue(branches.all { it.schema().isOptional })
    }

    @Test
    fun testToConnect_unionOfOnlyNull_isNoSchemaAtAll() {
        val jsonSchema = loadSchema("""{ "anyOf": [ { "type": "null" } ] }""")

        assertNull(jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema))
    }

    @Test
    fun testToConnect_nullableOneOfOfTwoRealTypes_isStillAnOptionalUnion() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "value": {
                            "oneOf": [ { "type": "string" }, { "type": "integer" }, { "type": "null" } ]
                        }
                    },
                    "additionalProperties": false
                }
                """.trimIndent(),
            )

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val field = connectSchema!!.field("value")
        assertEquals(Schema.Type.STRUCT, field.schema().type())
        assertTrue(field.schema().isOptional)
        assertEquals(2, field.schema().fields().size)
    }

    @Test
    fun testToConnect_integerWithFormat_isAFlatFieldNotAUnion() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}id": "https://example.com/weather-report.schema.json",
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "title": "WeatherReport",
                    "type": "object",
                    "properties": {
                        "location": { "type": "string" },
                        "temperature": { "type": "integer", "format": "int32" },
                        "timestamp": {
                            "description": "Timestamp in epoch format at which the weather was noted.",
                            "type": "integer",
                            "format": "int64"
                        }
                    },
                    "additionalProperties": false,
                    "required": [ "location", "temperature", "timestamp" ]
                }
                """.trimIndent(),
            )

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!

        assertEquals(Schema.Type.STRUCT, connectSchema.type())
        assertNull(connectSchema.name())
        assertEquals(Schema.Type.STRING, connectSchema.field("location").schema().type())
        assertEquals(Schema.Type.FLOAT64, connectSchema.field("temperature").schema().type())

        val timestamp = connectSchema.field("timestamp").schema()
        assertEquals(Schema.Type.FLOAT64, timestamp.type())
        assertNull(timestamp.name())
        assertFalse(timestamp.isOptional)
    }

    @Test
    fun testToConnect_integerWithFormat_carriesTheValue() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "timestamp": { "type": "integer", "format": "int64" }
                    },
                    "additionalProperties": false,
                    "required": [ "timestamp" ]
                }
                """.trimIndent(),
            )

        val jsonValue = ObjectMapper().readTree("""{ "timestamp": 1627335205 }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }
        assertEquals(1.627335205E9, (connectValue as Struct).get("timestamp"))
    }

    @Test
    fun testToConnect_integerWithFormatAndConnectType_keepsTheConnectType() {
        val jsonSchema =
            loadSchema("""{ "type": "integer", "format": "int64", "connect.type": "int64" }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!

        assertEquals(Schema.Type.INT64, connectSchema.type())
    }

    @Test
    fun testToConnect_objectWithFormat_isStillAStruct() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "type": "object",
                    "format": "custom",
                    "properties": { "value": { "type": "string" } },
                    "additionalProperties": false,
                    "required": [ "value" ]
                }
                """.trimIndent(),
            )

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!

        assertEquals(Schema.Type.STRUCT, connectSchema.type())
        assertEquals(Schema.Type.STRING, connectSchema.field("value").schema().type())
    }

    @Test
    fun testToConnect_nullableIntegerWithFormat_isAnOptionalField() {
        val jsonSchema =
            loadSchema("""{ "type": [ "integer", "null" ], "format": "int64" }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!

        assertEquals(Schema.Type.FLOAT64, connectSchema.type())
        assertTrue(connectSchema.isOptional)
    }

    @Test
    fun testToConnect_allOfWithTwoConstrainingSubschemas_isStillAUnionStruct() {
        val jsonSchema =
            loadSchema("""{ "allOf": [ { "type": "integer" }, { "minimum": 3 } ] }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)!!

        assertEquals(Schema.Type.STRUCT, connectSchema.type())
        assertEquals(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF, connectSchema.name())
        assertEquals(2, connectSchema.fields().size)
    }

    @Test
    fun testToConnect_constString_mapsToString() {
        val fieldSchema = convertConstProperty("""{ "const": "US" }""", """"US"""")

        assertEquals(Schema.Type.STRING, fieldSchema.type())
    }

    @Test
    fun testToConnect_constInteger_mapsToInt64() {
        val fieldSchema = convertConstProperty("""{ "const": 1 }""", "1")

        assertEquals(Schema.Type.INT64, fieldSchema.type())
    }

    @Test
    fun testToConnect_constNumber_mapsToFloat64() {
        val fieldSchema = convertConstProperty("""{ "const": 1.5 }""", "1.5")

        assertEquals(Schema.Type.FLOAT64, fieldSchema.type())
    }

    @Test
    fun testToConnect_constBoolean_mapsToBoolean() {
        val fieldSchema = convertConstProperty("""{ "const": true }""", "true")

        assertEquals(Schema.Type.BOOLEAN, fieldSchema.type())
    }

    @Test
    fun testToConnect_constObject_mapsToStruct() {
        val fieldSchema =
            convertConstProperty(
                """{ "const": { "code": "US", "rank": 1 } }""",
                """{ "code": "US", "rank": 1 }""",
            )

        assertEquals(Schema.Type.STRUCT, fieldSchema.type())
        assertEquals(Schema.Type.STRING, fieldSchema.field("code").schema().type())
        assertEquals(Schema.Type.INT64, fieldSchema.field("rank").schema().type())
    }

    @Test
    fun testToConnect_constArray_mapsToArrayOfElementType() {
        val fieldSchema = convertConstProperty("""{ "const": [ "a", "b" ] }""", """[ "a", "b" ]""")

        assertEquals(Schema.Type.ARRAY, fieldSchema.type())
        assertEquals(Schema.Type.STRING, fieldSchema.valueSchema().type())
    }

    @Test
    fun testToConnect_constNestedObjectAndArray_mapsRecursively() {
        val fieldSchema =
            convertConstProperty(
                """{ "const": { "name": "x", "tags": [ "a", "b" ] } }""",
                """{ "name": "x", "tags": [ "a", "b" ] }""",
            )

        assertEquals(Schema.Type.STRUCT, fieldSchema.type())
        assertEquals(Schema.Type.STRING, fieldSchema.field("name").schema().type())
        assertEquals(Schema.Type.ARRAY, fieldSchema.field("tags").schema().type())
        assertEquals(Schema.Type.STRING, fieldSchema.field("tags").schema().valueSchema().type())
    }

    @Test
    fun testToConnect_constObjectValue_deserializesToStruct() {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": { "region": { "const": { "code": "US", "rank": 1 } } },
                    "additionalProperties": false
                }
                """.trimIndent(),
            )
        val jsonValue = ObjectMapper().readTree("""{ "region": { "code": "US", "rank": 1 } }""")

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)

        assertDoesNotThrow { ConnectSchema.validateValue(connectSchema, connectValue) }

        val region = (connectValue as Struct).get("region") as Struct
        assertEquals("US", region.get("code"))
        assertEquals(1L, region.get("rank"))
    }

    @Test
    fun testToConnect_constEmptyArray_throwsClearError() {
        val exception =
            assertThrows(DataException::class.java) {
                convertConstProperty("""{ "const": [] }""", "[]")
            }

        assertTrue(
            exception.message!!.contains("empty 'const' array"),
            "Unexpected message: ${exception.message}",
        )
    }

    @Test
    fun testToConnect_constHeterogeneousArray_throwsClearError() {
        val exception =
            assertThrows(DataException::class.java) {
                convertConstProperty("""{ "const": [ "a", 1 ] }""", """[ "a", 1 ]""")
            }

        assertTrue(
            exception.message!!.contains("heterogeneous 'const' array"),
            "Unexpected message: ${exception.message}",
        )
    }

    private fun convertConstProperty(
        constDefinition: String,
        valueDefinition: String,
    ): Schema {
        val jsonSchema =
            loadSchema(
                """
                {
                    "${'$'}schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": { "c": $constDefinition },
                    "additionalProperties": false
                }
                """.trimIndent(),
            )

        val connectSchema = jsonSchemaToConnectSchemaConverter.toConnectSchema(jsonSchema)

        val jsonValue = ObjectMapper().readTree("""{ "c": $valueDefinition }""")
        val connectValue = jsonNodeToConnectValueConverter.toConnectValue(connectSchema, jsonValue)
        ConnectSchema.validateValue(connectSchema, connectValue)

        return connectSchema!!.field("c").schema()
    }

    private fun nullableSchema(types: String): String = """
        {
            "${'$'}schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "value": { "type": $types }
            },
            "additionalProperties": false
        }
    """.trimIndent()

    private fun loadSchema(definition: String): JsonSchema = SchemaLoader.load(JSONObject(definition))

    companion object {
        private val JSON_NODE_FACTORY: JsonNodeFactory = TypeConverter.JSON_NODE_FACTORY
    }
}
