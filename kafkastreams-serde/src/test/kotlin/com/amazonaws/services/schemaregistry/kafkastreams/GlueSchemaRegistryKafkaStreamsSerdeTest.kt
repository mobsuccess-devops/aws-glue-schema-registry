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

package com.amazonaws.services.schemaregistry.kafkastreams

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.kafkastreams.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User
import com.amazonaws.services.schemaregistry.kafkastreams.utils.json.Car
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Unit tests for testing GlueSchemaRegistryKafkaStreamsSerde class.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GlueSchemaRegistryKafkaStreamsSerdeTest {
    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    private lateinit var glueSchemaRegistryKafkaStreamsSerde: GlueSchemaRegistryKafkaStreamsSerde
    private lateinit var configs: Map<String, Any>

    /**
     * Test for AVRO generic record.
     */
    @Test
    fun testSerDe_AvroGenericRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.AVRO, AvroRecordType.GENERIC_RECORD)

        val expected = RecordGenerator.createGenericAvroRecord()
        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected)

        val serializer = createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.AVRO)
        val deserializer = createDeserializer(expected, avroGenericBytes)
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val genericRecord =
            glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(
                TEST_TOPIC,
                glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, expected),
            ) as GenericRecord

        assertEquals(expected, genericRecord)
    }

    /**
     * Test for AVRO specific record.
     */
    @Test
    fun testSerDe_AvroSpecificRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.AVRO, AvroRecordType.SPECIFIC_RECORD)

        val expected = RecordGenerator.createSpecificAvroRecord()
        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected)

        val serializer = createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.AVRO)
        val deserializer = createDeserializer(expected, avroSpecificBytes)
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val user =
            glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(
                TEST_TOPIC,
                glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, expected),
            ) as User

        assertEquals(expected, user)
    }

    /**
     * Test for JSON generic record.
     */
    @Test
    fun testSerDe_JsonGenericRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.JSON, AvroRecordType.GENERIC_RECORD)

        val expected = RecordGenerator.createGenericJsonRecord()

        val serializer = createSerializer(expected.schema!!, schemaVersionIdForTesting, DataFormat.JSON)
        val deserializer = createDeserializer(expected, jsonGenericBytes)
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val deserialized =
            glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(
                TEST_TOPIC,
                glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, expected),
            ) as JsonDataWithSchema

        assertEquals(expected, deserialized)
    }

    /**
     * Test for JSON specific record.
     */
    @Test
    fun testSerDe_JsonSpecificRecord_DeserializedEqualsSerialized() {
        configs = getProperties(DataFormat.JSON, AvroRecordType.GENERIC_RECORD)

        val expected = RecordGenerator.createSpecificJsonRecord()
        val jsonSerializer =
            JsonSerializer(
                GlueSchemaRegistryConfiguration(
                    mapOf(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2"),
                ),
            )
        val schemaDefinition = jsonSerializer.getSchemaDefinition(expected)

        val serializer = createSerializer(schemaDefinition, schemaVersionIdForTesting, DataFormat.JSON)
        val deserializer = createDeserializer(expected, jsonSpecificBytes)
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val car =
            glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(
                TEST_TOPIC,
                glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, expected),
            ) as Car

        assertEquals(expected, car)
    }

    /**
     * Test for null record.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testSerde_NullRecord_DeserializedEqualsSerializedAsNull(dataFormat: DataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD)
        val serializer = mock<GlueSchemaRegistryKafkaSerializer>()
        val deserializer = mock<GlueSchemaRegistryKafkaDeserializer>()
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val serialized = glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, null)
        val deserialized = glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(TEST_TOPIC, serialized)
        assertNull(serialized)
        assertNull(deserialized)
    }

    /**
     * Test for empty record.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testSerde_EmptyRecord_DeserializedEqualsSerializedAsNull(dataFormat: DataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD)
        val serializer = mock<GlueSchemaRegistryKafkaSerializer>()
        val deserializer = mock<GlueSchemaRegistryKafkaDeserializer>()
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)

        val serialized = glueSchemaRegistryKafkaStreamsSerde.serializer().serialize(TEST_TOPIC, "")
        val deserialized = glueSchemaRegistryKafkaStreamsSerde.deserializer().deserialize(TEST_TOPIC, serialized)
        assertNull(serialized)
        assertNull(deserialized)
    }

    /**
     * Tests the constructor with no parameters
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testConstructor_noParameters_succeeds(dataFormat: DataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD)
        glueSchemaRegistryKafkaStreamsSerde = GlueSchemaRegistryKafkaStreamsSerde()
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde)
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde.serializer())
        assertNotNull(glueSchemaRegistryKafkaStreamsSerde.deserializer())
    }

    /**
     * Tests invoking close method.
     */
    @ParameterizedTest
    @EnumSource(
        value = DataFormat::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["UNKNOWN_TO_SDK_VERSION", "PROTOBUF"],
    )
    fun testClose_succeeds(dataFormat: DataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD)
        glueSchemaRegistryKafkaStreamsSerde = createTestGlueSchemaRegistryKafkaStreamsSerde(dataFormat)
        assertDoesNotThrow { glueSchemaRegistryKafkaStreamsSerde.close() }
    }

    /**
     * Test the invocation of configure method
     */
    @ParameterizedTest
    @EnumSource(
        value = DataFormat::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["UNKNOWN_TO_SDK_VERSION", "PROTOBUF"],
    )
    fun testConfigure_succeeds(dataFormat: DataFormat) {
        configs = getProperties(dataFormat, AvroRecordType.GENERIC_RECORD)
        glueSchemaRegistryKafkaStreamsSerde = createTestGlueSchemaRegistryKafkaStreamsSerde(dataFormat)
        assertDoesNotThrow { glueSchemaRegistryKafkaStreamsSerde.configure(configs, false) }
    }

    /**
     * To create a GlueSchemaRegistryKafkaSerializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaSerializer instance
     */
    private fun createSerializer(
        schemaDefinition: String,
        schemaVersionId: UUID,
        dataFormat: DataFormat,
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
                eq(SCHEMA_NAME),
                eq(dataFormat.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(schemaVersionId)
        val serializer = GlueSchemaRegistryKafkaSerializer(mockCredProvider, null)
        serializer.configure(configs, true)

        serializer.glueSchemaRegistrySerializationFacade = facade

        return serializer
    }

    /**
     * To create a GlueSchemaRegistryKafkaDeserializer instance with mocked parameters.
     *
     * @return a mocked GlueSchemaRegistryKafkaDeserializer instance
     */
    private fun createDeserializer(
        record: Any,
        bytes: ByteArray,
    ): GlueSchemaRegistryKafkaDeserializer {
        val facade = mock<GlueSchemaRegistryDeserializationFacade>()
        val awsDeserializerInput =
            AWSDeserializerInput
                .builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(TEST_TOPIC)
                .build()

        whenever(facade.deserialize(awsDeserializerInput)).thenReturn(record)
        val deserializer = GlueSchemaRegistryKafkaDeserializer(mockCredProvider, null)
        deserializer.configure(configs, true)

        deserializer.glueSchemaRegistryDeserializationFacade = facade

        return deserializer
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configurations
     */
    private fun getProperties(
        dataFormat: DataFormat,
        recordType: AvroRecordType,
    ): Map<String, Any> = mapOf(
        AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
        AWSSchemaRegistryConstants.AWS_ENDPOINT to "https://test",
        AWSSchemaRegistryConstants.SCHEMA_NAME to SCHEMA_NAME,
        AWSSchemaRegistryConstants.DATA_FORMAT to dataFormat.name,
        // Only required for AVRO case
        AWSSchemaRegistryConstants.AVRO_RECORD_TYPE to recordType.name,
    )

    private fun createTestGlueSchemaRegistryKafkaStreamsSerde(dataFormat: DataFormat): GlueSchemaRegistryKafkaStreamsSerde {
        val record: Any
        val schemaDefinition: String
        val expectedBytes: ByteArray
        when (dataFormat) {
            DataFormat.AVRO -> {
                record = RecordGenerator.createGenericAvroRecord()
                schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(record)
                expectedBytes = avroGenericBytes
            }

            DataFormat.JSON -> {
                record = RecordGenerator.createGenericJsonRecord()
                schemaDefinition = record.schema!!
                expectedBytes = jsonGenericBytes
            }

            else -> throw RuntimeException("Data format is not supported")
        }

        val serializer = createSerializer(schemaDefinition, schemaVersionIdForTesting, dataFormat)
        val deserializer = createDeserializer(record, expectedBytes)
        return GlueSchemaRegistryKafkaStreamsSerde(serializer, deserializer)
    }

    companion object {
        private const val TEST_TOPIC = "test-topic"
        private const val SCHEMA_NAME = "User-Topic"
        private val schemaVersionIdForTesting: UUID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        private val avroGenericBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 10, 115, 97,
                110, 115, 97, 0, -58, 1, 0, 6, 114, 101, 100,
            )
        private val avroSpecificBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 8, 116, 101,
                115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116,
            )
        private val jsonGenericBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 123, 34,
                102, 105, 114, 115, 116, 78, 97, 109, 101, 34, 58, 34, 74, 111, 104, 110, 34, 44, 34, 108, 97,
                115, 116, 78, 97, 109, 101, 34, 58, 34, 68, 111, 101, 34, 44, 34, 97, 103, 101, 34, 58, 50, 49,
                125,
            )
        private val jsonSpecificBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5, 93, -23, -17, 12, 99, 123, 34,
                109, 97, 107, 101, 34, 58, 34, 72, 111, 110, 100, 97, 34, 44, 34, 109, 111, 100, 101, 108, 34, 58,
                34, 99, 114, 118, 34, 44, 34, 117, 115, 101, 100, 34, 58, 116, 114, 117, 101, 44, 34, 109, 105,
                108, 101, 115, 34, 58, 49, 48, 48, 48, 48, 44, 34, 121, 101, 97, 114, 34, 58, 50, 48, 49, 54, 44,
                34, 112, 117, 114, 99, 104, 97, 115, 101, 68, 97, 116, 101, 34, 58, 57, 52, 54, 54, 56, 52, 56,
                48, 48, 48, 48, 48, 44, 34, 108, 105, 115, 116, 101, 100, 68, 97, 116, 101, 34, 58, 49, 51, 57,
                50, 49, 48, 53, 54, 48, 48, 48, 48, 48, 44, 34, 111, 119, 110, 101, 114, 115, 34, 58, 91, 34, 74,
                111, 104, 110, 34, 44, 34, 74, 97, 110, 101, 34, 44, 34, 72, 117, 34, 93, 44, 34, 115, 101, 114,
                118, 105, 99, 101, 67, 104, 101, 99, 107, 115, 34, 58, 91, 53, 48, 48, 48, 46, 48, 44, 49, 48, 55,
                56, 48, 46, 51, 93, 125,
            )
    }
}
