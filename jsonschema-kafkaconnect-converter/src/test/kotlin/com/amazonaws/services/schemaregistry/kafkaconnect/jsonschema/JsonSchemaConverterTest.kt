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

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.DecimalFormat
import org.everit.json.schema.BooleanSchema
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import java.nio.ByteBuffer
import java.util.UUID
import org.everit.json.schema.Schema as JsonSchema

/**
 * Unit tests for testing JsonSchemaConverter class.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonSchemaConverterTest {
    @Mock
    lateinit var mockGsrKafkaSerializer: GlueSchemaRegistryKafkaSerializer

    @Mock
    lateinit var mockGsrKafkaDeserializer: GlueSchemaRegistryKafkaDeserializer

    @Mock
    private lateinit var mockClient: AWSSchemaRegistryClient

    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    private lateinit var converter: JsonSchemaConverter
    private lateinit var configs: Map<String, Any>

    @BeforeEach
    fun setUp() {
        configs = properties
    }

    /**
     * Test for JsonSchemaConverter config method.
     */
    @Test
    fun testConverter_configure_notNull() {
        converter = JsonSchemaConverter()
        converter.configure(properties, false)
        assertNotNull(converter)
        assertNotNull(converter.serializer)
        assertNotNull(converter.deserializer)
        assertNotNull(converter.connectSchemaToJsonSchemaConverter)
        assertNotNull(converter.connectValueToJsonNodeConverter)
        assertNotNull(converter.jsonNodeToConnectValueConverter)
        assertNotNull(converter.jsonSchemaToConnectSchemaConverter)
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testSchemaAndValueArgumentsProvider",
    )
    fun testConverter_fromConnectData_equalsToConnectData(
        jsonSchema: JsonSchema,
        connectSchema: Schema?,
        jsonValue: JsonNode,
        connectValue: Any?,
    ) {
        val expected = SchemaAndValue(connectSchema, connectValue)

        val testSchemaVersionId = UUID.randomUUID()

        val canonicalSchema = JsonSchemaConverter.canonicalize(jsonSchema.toString())
        val gsrKafkaSerializer = createSerializer(canonicalSchema, testSchemaVersionId)
        val gsrKafkaDeserializer = createDeserializer(canonicalSchema, testSchemaVersionId)

        converter = JsonSchemaConverter(gsrKafkaSerializer, gsrKafkaDeserializer)
        converter.configure(properties, false)

        val serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value())

        val actual = converter.toConnectData(TEST_TOPIC, serializedData)

        if (!jsonValue.isNull || !jsonSchema.hasDefaultValue()) {
            assertEquals(expected.schema(), actual.schema())
        }

        if (expected.value() != null &&
            expected.value().javaClass.isArray &&
            Schema.Type.BYTES == connectSchema!!.type()
        ) {
            assertArrayEquals(expected.value() as ByteArray, actual.value() as ByteArray)
        } else if (!jsonValue.isNull || !jsonSchema.hasDefaultValue()) {
            assertEquals(expected.value(), actual.value())
        }
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testSchemaAndValueArgumentsProvider",
    )
    fun testConverter_fromConnectData_throwsException(
        jsonSchema: JsonSchema,
        connectSchema: Schema?,
        jsonValue: JsonNode,
        connectValue: Any?,
    ) {
        // This test should just validate that the right Exception is thrown
        // Skipping Array because the ordering changes for Arrays that represent maps
        // with "key" and "value"
        if (!jsonValue.isArray && (!jsonValue.isNull || !jsonSchema.hasDefaultValue())) {
            val expected = SchemaAndValue(connectSchema, connectValue)

            converter = JsonSchemaConverter(mockGsrKafkaSerializer, mockGsrKafkaDeserializer)
            converter.configure(properties, false)

            val jsonSchemaWithData: Any =
                JsonDataWithSchema
                    .builder(JsonSchemaConverter.canonicalize(jsonSchema.toString()), jsonValue.toString())
                    .build()

            whenever(mockGsrKafkaSerializer.serialize(TEST_TOPIC, jsonSchemaWithData))
                .thenThrow(AWSSchemaRegistryException())

            assertThrows(DataException::class.java) {
                converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value())
            }
        }
    }

    @ParameterizedTest
    @MethodSource(
        "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.TestDataProvider#" +
            "testSchemaAndValueArgumentsProvider",
    )
    fun testConverter_toConnectData_throwsException(
        jsonSchema: JsonSchema,
        connectSchema: Schema?,
        jsonValue: JsonNode,
        connectValue: Any?,
    ) {
        val expected = SchemaAndValue(connectSchema, connectValue)

        val testSchemaVersionId = UUID.randomUUID()

        val gsrKafkaSerializer =
            createSerializer(JsonSchemaConverter.canonicalize(jsonSchema.toString()), testSchemaVersionId)

        converter = JsonSchemaConverter(gsrKafkaSerializer, mockGsrKafkaDeserializer)
        converter.configure(properties, false)

        val serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected.value())

        whenever(mockGsrKafkaDeserializer.deserialize(TEST_TOPIC, serializedData))
            .thenThrow(AWSSchemaRegistryException())

        assertThrows(DataException::class.java) { converter.toConnectData(TEST_TOPIC, serializedData) }
    }

    /**
     * Test that the JSON Schema is parsed once per schema definition and reused afterwards,
     * while the registry lookup that produces the definition still happens on every record.
     */
    @Test
    fun testConverter_toConnectData_reusesTheParsedJsonSchema() {
        val facade = createFacade(STRING_SCHEMA_DEFINITION, A_STRING_PAYLOAD)
        converter = JsonSchemaConverter(mockGsrKafkaSerializer, deserializerBackedBy(facade))
        converter.configure(properties, false)

        val first = converter.toConnectData(TEST_TOPIC, GSR_BYTES)
        val parsed = converter.parsedSchemaCache.getIfPresent(STRING_SCHEMA_DEFINITION)
        val second = converter.toConnectData(TEST_TOPIC, GSR_BYTES)

        assertNotNull(parsed)
        assertSame(parsed, converter.parsedSchemaCache.getIfPresent(STRING_SCHEMA_DEFINITION))
        assertEquals(first, second)
        verify(facade, times(2)).getSchemaDefinition(eq(GSR_BYTES))
    }

    /**
     * Test that a cached entry, rather than the definition string, is what the conversion reads:
     * a boolean schema parked under the key of a string definition yields a boolean Connect schema.
     */
    @Test
    fun testConverter_toConnectData_convertsFromTheCachedSchema() {
        val facade = createFacade(STRING_SCHEMA_DEFINITION, "true")
        converter = JsonSchemaConverter(mockGsrKafkaSerializer, deserializerBackedBy(facade))
        converter.configure(properties, false)
        converter.parsedSchemaCache.put(STRING_SCHEMA_DEFINITION, BooleanSchema.builder().build())

        val actual = converter.toConnectData(TEST_TOPIC, GSR_BYTES)

        assertEquals(Schema.Type.BOOLEAN, actual.schema().type())
        assertEquals(true, actual.value())
    }

    /**
     * Test that a malformed schema definition raises the same exception it always did, and that
     * the failure is not remembered.
     */
    @Test
    fun testConverter_toConnectData_malformedSchemaThrowsAndIsNotCached() {
        val facade = createFacade(MALFORMED_SCHEMA_DEFINITION, A_STRING_PAYLOAD)
        converter = JsonSchemaConverter(mockGsrKafkaSerializer, deserializerBackedBy(facade))
        converter.configure(properties, false)

        val exception =
            assertThrows(DataException::class.java) { converter.toConnectData(TEST_TOPIC, GSR_BYTES) }

        assertEquals("Failed to read JSON Schema : $MALFORMED_SCHEMA_DEFINITION", exception.message)
        assertNull(converter.parsedSchemaCache.getIfPresent(MALFORMED_SCHEMA_DEFINITION))
        assertThrows(DataException::class.java) { converter.toConnectData(TEST_TOPIC, GSR_BYTES) }
    }

    /**
     * To create a mocked deserialization facade answering with a fixed schema definition and
     * payload, so that a record can be converted without a registry behind it.
     *
     * @return a mocked GlueSchemaRegistryDeserializationFacade instance
     */
    private fun createFacade(
        schemaDefinition: String,
        payload: String,
    ): GlueSchemaRegistryDeserializationFacade {
        val facade = mock<GlueSchemaRegistryDeserializationFacade>()
        val input =
            AWSDeserializerInput
                .builder()
                .buffer(ByteBuffer.wrap(GSR_BYTES))
                .transportName(TEST_TOPIC)
                .build()

        whenever(facade.deserialize(input))
            .thenReturn(JsonDataWithSchema.builder(schemaDefinition, payload).build())
        whenever(facade.getSchemaDefinition(eq(GSR_BYTES))).thenReturn(schemaDefinition)

        return facade
    }

    private fun deserializerBackedBy(
        facade: GlueSchemaRegistryDeserializationFacade,
    ): GlueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer(mockCredProvider, null).apply {
        glueSchemaRegistryDeserializationFacade = facade
    }

    /**
     * To create a GlueSchemaRegistryKafkaSerializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaSerializer instance
     */
    private fun createSerializer(
        schemaDefinition: String,
        schemaVersionId: UUID,
    ): GlueSchemaRegistryKafkaSerializer {
        val facade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(mockCredProvider)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq(TEST_TOPIC),
                eq(DataFormat.JSON.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(schemaVersionId)
        val gsKafkaSerializer = GlueSchemaRegistryKafkaSerializer(mockCredProvider, null)

        gsKafkaSerializer.glueSchemaRegistrySerializationFacade = facade

        return gsKafkaSerializer
    }

    /**
     * To create a GlueSchemaRegistryKafkaDeserializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaDeserializer instance
     */
    private fun createDeserializer(
        schemaDefinition: String,
        schemaVersionID: UUID,
    ): GlueSchemaRegistryKafkaDeserializer {
        val facade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(mockCredProvider)
                .schemaRegistryClient(mockClient)
                .build()

        val getSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(schemaDefinition)
                .dataFormat(DataFormat.JSON)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        whenever(mockClient.getSchemaVersionResponse(schemaVersionID.toString()))
            .thenReturn(getSchemaVersionResponse)
        val deserializer = GlueSchemaRegistryKafkaDeserializer(mockCredProvider, null)

        deserializer.glueSchemaRegistryDeserializationFacade = facade

        return deserializer
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private val properties: Map<String, Any>
        get() =
            mapOf(
                AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
                AWSSchemaRegistryConstants.AWS_ENDPOINT to "https://test",
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
                AWSSchemaRegistryConstants.DATA_FORMAT to DataFormat.JSON.name,
                JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG to DecimalFormat.NUMERIC.name,
                AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES to
                    listOf(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS.name),
                AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES to
                    listOf(SerializationFeature.INDENT_OUTPUT.name),
            )

    companion object {
        private const val TEST_TOPIC = "User-Topic"
        private const val STRING_SCHEMA_DEFINITION = """{"type":"string"}"""
        private const val MALFORMED_SCHEMA_DEFINITION = "not a JSON Schema"
        private const val A_STRING_PAYLOAD = "\"a string\""
        private val GSR_BYTES =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100,
            )
        private const val TEST_SCHEMA_ARN =
            "arn:aws:glue:us-east-1:111111111111:schema/registry_name/$TEST_TOPIC"
    }
}
