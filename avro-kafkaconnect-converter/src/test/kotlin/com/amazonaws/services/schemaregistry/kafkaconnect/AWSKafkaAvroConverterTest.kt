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

package com.amazonaws.services.schemaregistry.kafkaconnect

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_ARN
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Unit tests for testing AWSKafkaAvroConverter class.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AWSKafkaAvroConverterTest {
    @Mock
    private lateinit var mockAvroDataConfig: AvroDataConfig

    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    @Mock
    private lateinit var awsKafkaAvroSerializer: AWSKafkaAvroSerializer

    @Mock
    private lateinit var awsKafkaAvroDeserializer: AWSKafkaAvroDeserializer

    private lateinit var converter: AWSKafkaAvroConverter
    private lateinit var configs: MutableMap<String, Any>
    private lateinit var avroData: AvroData

    @BeforeEach
    fun setUp() {
        avroData = AvroData(mockAvroDataConfig)
        configs = getProperties()
    }

    /**
     * Test for AWSKafkaAvroConverter config method.
     */
    @Test
    fun testFromConnectData_beforeConfigure_saysConfigureWasNotCalled() {
        val converter = AWSKafkaAvroConverter()

        val thrown =
            assertThrows(IllegalStateException::class.java) {
                converter.fromConnectData("User-Topic", null, "some record")
            }

        assertTrue(thrown.message!!.contains("configure()"), thrown.message)
    }

    @Test
    fun testConverter_configure() {
        converter = AWSKafkaAvroConverter()
        converter.configure(getProperties(), false)
        assertNotNull(converter)
        assertNotNull(converter.serializer)
        assertNotNull(converter.deserializer)
        assertNotNull(converter.avroData)
    }

    /**
     * Test for Struct record.
     */
    @Test
    fun testConverter_fromConnectData_equalsToConnectData() {
        val expected = createStructRecord()
        val avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString()
        val avroData = this.avroData.fromConnectData(expected.schema(), expected)

        val awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, SCHEMA_VERSION_ID_FOR_TESTING)
        val awsKafkaAvroDeserializer = createDeserializer(avroData, GENERIC_BYTES, avroSchemaDefinition)
        converter = AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData)

        val serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected)
        val structRecord = converter.toConnectData(TEST_TOPIC, serializedData)

        assertEquals(expected, structRecord!!.value())
    }

    /**
     * Test AWSKafkaAvroConverter when serializer throws exception.
     */
    @Test
    fun testConverter_fromConnectData_throwsException() {
        val expected = createStructRecord()
        val avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString()
        val avroData = this.avroData.fromConnectData(expected.schema(), expected)

        whenever(awsKafkaAvroSerializer.serialize(TEST_TOPIC, avroData)).thenThrow(AWSSchemaRegistryException())
        val awsKafkaAvroDeserializer = createDeserializer(avroData, GENERIC_BYTES, avroSchemaDefinition)
        converter = AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData)

        assertThrows(DataException::class.java) {
            converter.fromConnectData(TEST_TOPIC, expected.schema(), expected)
        }
    }

    /**
     * Test AWSKafkaAvroConverter when de-serializer throws exception.
     */
    @Test
    fun testConverter_toConnectData_throwsException() {
        val expected = createStructRecord()
        val avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString()

        val awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, SCHEMA_VERSION_ID_FOR_TESTING)
        whenever(awsKafkaAvroDeserializer.deserialize(TEST_TOPIC, GENERIC_BYTES))
            .thenThrow(AWSSchemaRegistryException())
        converter = AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, avroData)
        val serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected)

        assertThrows(DataException::class.java) { converter.toConnectData(TEST_TOPIC, serializedData) }
    }

    /**
     * Test AWSKafkaAvroConverter when value is null.
     */
    @Test
    fun testConverter_toConnectData_NullValue() {
        converter = spy(AWSKafkaAvroConverter())
        assertEquals(SchemaAndValue.NULL, converter.toConnectData(TEST_TOPIC, null))
    }

    @Test
    fun testConverter_fromConnectData_NullValue_returnsNull() {
        converter =
            AWSKafkaAvroConverter(
                AWSKafkaAvroSerializer(mockCredProvider, null),
                awsKafkaAvroDeserializer,
                avroData,
            )

        val optionalSchema =
            SchemaBuilder
                .struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build()

        assertNull(converter.fromConnectData(TEST_TOPIC, optionalSchema, null))
        assertNull(converter.fromConnectData(TEST_TOPIC, null, null))
    }

    /**
     * Test AWSKafkaAvroConverter with assume role.
     */
    @Test
    fun testConverter_configure_invokeAssumeRoleWithCustomSession() {
        configs[ASSUME_ROLE_ARN] = ROLE_ARN
        configs[ASSUME_ROLE_SESSION_NAME] = "my-session"

        converter = spy(AWSKafkaAvroConverter())
        doReturn(mockCredProvider)
            .whenever(converter)
            .getCredentialsProvider(any(), any(), any())

        converter.configure(configs, true)

        verify(converter).getCredentialsProvider(ROLE_ARN, "my-session", REGION)
        assertTrue(converter.isKey)
        assertNotNull(converter.serializer)
        assertNotNull(converter.deserializer)
        assertNotNull(converter.avroData)
    }

    /**
     * Test AWSKafkaAvroConverter assume role, default session name.
     */
    @Test
    fun testConverter_configure_defaultSessionNameForAssumeRole() {
        configs[ASSUME_ROLE_ARN] = ROLE_ARN

        converter = spy(AWSKafkaAvroConverter())
        doReturn(mockCredProvider)
            .whenever(converter)
            .getCredentialsProvider(any(), any(), any())

        converter.configure(configs, false)

        verify(converter).getCredentialsProvider(ROLE_ARN, "kafka-connect-session", REGION)
        assertFalse(converter.isKey)
    }

    /**
     * Test AWSKafkaAvroConverter assume role empty.
     */
    @Test
    fun testConverter_configure_noAssumeRoleIfArnIsEmpty() {
        configs[ASSUME_ROLE_ARN] = ""

        converter = spy(AWSKafkaAvroConverter())
        converter.configure(configs, false)

        verify(converter, never())
            .getCredentialsProvider(any(), any(), any())
    }

    /**
     * Test AWSKafkaAvroConverter assume role null.
     */
    @Test
    fun testConverter_configure_noAssumeRoleIfArnIsNotProvided() {
        converter = spy(AWSKafkaAvroConverter())
        converter.configure(getProperties(), false)

        verify(converter, never())
            .getCredentialsProvider(any(), any(), any())
    }

    /**
     * Test that the fix for secondary deserializer schema extraction works with GSR data.
     * This ensures backward compatibility is maintained.
     */
    @Test
    fun testConverter_toConnectData_GSRData_BackwardCompatibility() {
        val expected = createStructRecord()
        val avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString()
        val avroData = this.avroData.fromConnectData(expected.schema(), expected)

        val awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, SCHEMA_VERSION_ID_FOR_TESTING)
        val awsKafkaAvroDeserializer = createDeserializer(avroData, GENERIC_BYTES, avroSchemaDefinition)

        // Mock canDeserialize to return true for GSR data
        whenever(awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade!!.canDeserialize(GENERIC_BYTES))
            .thenReturn(true)

        converter = AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData)

        val serializedData = converter.fromConnectData(TEST_TOPIC, expected.schema(), expected)
        val structRecord = converter.toConnectData(TEST_TOPIC, serializedData)

        assertEquals(expected, structRecord!!.value())

        // Verify that GSR schema extraction was used
        verify(awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade!!).canDeserialize(GENERIC_BYTES)
        verify(awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade!!)
            .getSchemaDefinition(eq(GENERIC_BYTES))
    }

    /**
     * Test that the Avro schema is parsed once per schema definition and reused afterwards,
     * while the registry lookup still happens on every record.
     */
    @Test
    fun testConverter_extractAvroSchema_reusesTheParsedSchema() {
        val expected = createStructRecord()
        val avroSchemaDefinition = avroData.fromConnectSchema(expected.schema()).toString()
        val avroRecord = this.avroData.fromConnectData(expected.schema(), expected)

        val awsKafkaAvroSerializer = createSerializer(avroSchemaDefinition, SCHEMA_VERSION_ID_FOR_TESTING)
        val awsKafkaAvroDeserializer = createDeserializer(avroRecord, GENERIC_BYTES, avroSchemaDefinition)

        whenever(awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade!!.canDeserialize(GENERIC_BYTES))
            .thenReturn(true)

        converter = AWSKafkaAvroConverter(awsKafkaAvroSerializer, awsKafkaAvroDeserializer, this.avroData)

        val first = converter.extractAvroSchema(GENERIC_BYTES, avroRecord)
        val second = converter.extractAvroSchema(GENERIC_BYTES, avroRecord)

        assertEquals(avroSchemaDefinition, first.toString())
        assertSame(first, second)
        verify(awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade!!, times(2))
            .getSchemaDefinition(eq(GENERIC_BYTES))
    }

    /**
     * To create a AWSKafkaAvroSerializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroSerializer instance
     */
    private fun createSerializer(
        schemaDefinition: String,
        schemaVersionId: UUID,
    ): AWSKafkaAvroSerializer {
        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(mockCredProvider)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq("User-Topic"),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(schemaVersionId)
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(mockCredProvider, null)
        awsKafkaAvroSerializer.configure(configs, true)

        awsKafkaAvroSerializer.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade

        return awsKafkaAvroSerializer
    }

    /**
     * To create a AWSKafkaAvroDeserializer instance with mocked parameters.
     *
     * @return a mocked AWSKafkaAvroDeserializer instance
     */
    private fun createDeserializer(
        record: Any?,
        bytes: ByteArray,
        schemaDefinition: String,
    ): AWSKafkaAvroDeserializer {
        val glueSchemaRegistryDeserializationFacade = mock<GlueSchemaRegistryDeserializationFacade>()
        val awsDeserializerInput =
            AWSDeserializerInput
                .builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(TEST_TOPIC)
                .build()

        whenever(glueSchemaRegistryDeserializationFacade.deserialize(awsDeserializerInput)).thenReturn(record)
        whenever(glueSchemaRegistryDeserializationFacade.getSchemaDefinition(eq(bytes))).thenReturn(schemaDefinition)
        val awsKafkaAvroDeserializer = AWSKafkaAvroDeserializer(mockCredProvider, null)
        awsKafkaAvroDeserializer.configure(configs, true)

        awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade

        return awsKafkaAvroDeserializer
    }

    /**
     * To create a Connect Struct record.
     *
     * @return a Connect Struct
     */
    private fun createStructRecord(): Struct {
        val schema =
            SchemaBuilder
                .struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("favorite_number", Schema.INT32_SCHEMA)
                .field("favorite_color", Schema.STRING_SCHEMA)
                .build()

        return Struct(schema)
            .put("name", "sansa")
            .put("favorite_number", 99)
            .put("favorite_color", "red")
    }

    /**
     * To create a map of configurations.
     *
     * @return a map of configuratons
     */
    private fun getProperties(): MutableMap<String, Any> {
        val props = HashMap<String, Any>()

        props[AWSSchemaRegistryConstants.AWS_REGION] = REGION
        props[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        props[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
        props[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()

        return props
    }

    companion object {
        private const val TEST_TOPIC = "User-Topic"
        private val SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        private val GENERIC_BYTES =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, -58, 1, 6, 114, 101, 100,
            )
        private const val ROLE_ARN = "arn:aws:iam::123456789012:role/my-role"
        private const val REGION = "us-west-2"
    }
}
