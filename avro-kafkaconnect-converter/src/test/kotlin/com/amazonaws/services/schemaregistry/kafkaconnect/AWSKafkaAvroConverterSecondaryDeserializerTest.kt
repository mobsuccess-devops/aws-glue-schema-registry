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

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * Unit tests for AWSKafkaAvroConverter secondary deserializer functionality.
 * Tests the fix for the bug where secondary deserializer data was incorrectly
 * processed through GSR schema extraction.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AWSKafkaAvroConverterSecondaryDeserializerTest {
    @Mock
    private lateinit var mockSerializer: AWSKafkaAvroSerializer

    @Mock
    private lateinit var mockDeserializer: AWSKafkaAvroDeserializer

    @Mock
    private lateinit var mockDeserializationFacade: GlueSchemaRegistryDeserializationFacade

    @Mock
    private lateinit var mockAvroData: AvroData

    @Mock
    private lateinit var mockCredentialsProvider: AwsCredentialsProvider

    @Mock
    private lateinit var mockGenericRecord: GenericRecord

    @Mock
    private lateinit var mockSpecificRecord: SpecificRecord

    private lateinit var converter: AWSKafkaAvroConverter

    @BeforeEach
    fun setUp() {
        converter = AWSKafkaAvroConverter(mockSerializer, mockDeserializer, mockAvroData)
        whenever(mockDeserializer.glueSchemaRegistryDeserializationFacade).thenReturn(mockDeserializationFacade)
    }

    /**
     * Test successful conversion of GSR data using GSR schema extraction.
     */
    @Test
    fun testToConnectData_GSRData_UsesGSRSchemaExtraction() {
        // Arrange
        whenever(mockDeserializer.deserialize(TEST_TOPIC, GSR_DATA)).thenReturn(mockGenericRecord)
        whenever(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true)
        whenever(mockDeserializationFacade.getSchemaDefinition(GSR_DATA)).thenReturn(AVRO_SCHEMA_STRING)
        whenever(mockAvroData.toConnectData(any<Schema>(), eq(mockGenericRecord)))
            .thenReturn(SchemaAndValue(null, "test-result"))

        // Act
        val result = converter.toConnectData(TEST_TOPIC, GSR_DATA)

        // Assert
        assertNotNull(result)
        assertEquals("test-result", result!!.value())
        verify(mockDeserializationFacade).canDeserialize(GSR_DATA)
        verify(mockDeserializationFacade).getSchemaDefinition(eq(GSR_DATA))
        verify(mockAvroData).toConnectData(any<Schema>(), eq(mockGenericRecord))
    }

    /**
     * Test successful conversion of secondary deserializer data using Avro object schema extraction.
     */
    @Test
    fun testToConnectData_SecondaryDeserializerData_UsesAvroObjectSchemaExtraction() {
        // Arrange
        whenever(mockDeserializer.deserialize(TEST_TOPIC, CONFLUENT_DATA)).thenReturn(mockGenericRecord)
        whenever(mockDeserializationFacade.canDeserialize(CONFLUENT_DATA)).thenReturn(false)
        whenever(mockGenericRecord.schema).thenReturn(AVRO_SCHEMA)
        whenever(mockAvroData.toConnectData(AVRO_SCHEMA, mockGenericRecord))
            .thenReturn(SchemaAndValue(null, "confluent-result"))

        // Act
        val result = converter.toConnectData(TEST_TOPIC, CONFLUENT_DATA)

        // Assert
        assertNotNull(result)
        assertEquals("confluent-result", result!!.value())
        verify(mockDeserializationFacade).canDeserialize(CONFLUENT_DATA)
        verify(mockDeserializationFacade, never()).getSchemaDefinition(any<ByteArray>())
        verify(mockGenericRecord).schema
        verify(mockAvroData).toConnectData(AVRO_SCHEMA, mockGenericRecord)
    }

    /**
     * Test that GSR schema extraction failure is properly handled.
     */
    @Test
    fun testToConnectData_GSRSchemaExtractionFails_ThrowsDataException() {
        // Arrange
        whenever(mockDeserializer.deserialize(TEST_TOPIC, GSR_DATA)).thenReturn(mockGenericRecord)
        whenever(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true)
        whenever(mockDeserializationFacade.getSchemaDefinition(any<ByteArray>()))
            .thenThrow(AWSSchemaRegistryException("Schema not found"))

        // Act & Assert
        val exception =
            assertThrows(DataException::class.java) {
                converter.toConnectData(TEST_TOPIC, GSR_DATA)
            }
        assertTrue(exception.message!!.contains("Failed to extract schema from GSR metadata"))
        assertTrue(exception.cause is AWSSchemaRegistryException)
    }

    /**
     * Test extraction of schema from GenericRecord.
     */
    @Test
    fun testExtractSchemaFromAvroObject_GenericRecord_ReturnsSchema() {
        // Arrange
        whenever(mockGenericRecord.schema).thenReturn(AVRO_SCHEMA)

        // Act
        val result = converter.extractSchemaFromAvroObject(mockGenericRecord)

        // Assert
        assertEquals(AVRO_SCHEMA, result)
        verify(mockGenericRecord).schema
    }

    /**
     * Test extraction of schema from SpecificRecord.
     */
    @Test
    fun testExtractSchemaFromAvroObject_SpecificRecord_ReturnsSchema() {
        // Arrange
        whenever(mockSpecificRecord.schema).thenReturn(AVRO_SCHEMA)

        // Act
        val result = converter.extractSchemaFromAvroObject(mockSpecificRecord)

        // Assert
        assertEquals(AVRO_SCHEMA, result)
        verify(mockSpecificRecord).schema
    }

    /**
     * Test that invalid Avro object types throw appropriate exception.
     */
    @Test
    fun testExtractSchemaFromAvroObject_InvalidType_ThrowsDataException() {
        // Arrange
        val invalidObject = "not-an-avro-record"

        // Act & Assert
        val exception =
            assertThrows(DataException::class.java) {
                converter.extractSchemaFromAvroObject(invalidObject)
            }
        assertTrue(exception.message!!.contains("Deserialized object is not a valid Avro record"))
        assertTrue(exception.message!!.contains("java.lang.String"))
    }

    /**
     * Test that null Avro object throws appropriate exception.
     */
    @Test
    fun testExtractSchemaFromAvroObject_NullObject_ThrowsDataException() {
        // Act & Assert
        val exception =
            assertThrows(DataException::class.java) {
                converter.extractSchemaFromAvroObject(null)
            }
        assertTrue(exception.message!!.contains("Deserialized object is not a valid Avro record"))
        assertTrue(exception.message!!.contains("null"))
    }

    /**
     * Test extractAvroSchema method with GSR data.
     */
    @Test
    fun testExtractAvroSchema_GSRData_CallsGSRSchemaExtraction() {
        // Arrange
        whenever(mockDeserializationFacade.canDeserialize(GSR_DATA)).thenReturn(true)
        whenever(mockDeserializationFacade.getSchemaDefinition(any<ByteArray>())).thenReturn(AVRO_SCHEMA_STRING)

        // Act
        val result = converter.extractAvroSchema(GSR_DATA, mockGenericRecord)

        // Assert
        assertEquals(AVRO_SCHEMA.toString(), result.toString())
        verify(mockDeserializationFacade).canDeserialize(GSR_DATA)
        verify(mockDeserializationFacade).getSchemaDefinition(eq(GSR_DATA))
        verify(mockGenericRecord, never()).schema
    }

    /**
     * Test extractAvroSchema method with secondary deserializer data.
     */
    @Test
    fun testExtractAvroSchema_SecondaryData_CallsAvroObjectSchemaExtraction() {
        // Arrange
        whenever(mockDeserializationFacade.canDeserialize(CONFLUENT_DATA)).thenReturn(false)
        whenever(mockGenericRecord.schema).thenReturn(AVRO_SCHEMA)

        // Act
        val result = converter.extractAvroSchema(CONFLUENT_DATA, mockGenericRecord)

        // Assert
        assertEquals(AVRO_SCHEMA, result)
        verify(mockDeserializationFacade).canDeserialize(CONFLUENT_DATA)
        verify(mockDeserializationFacade, never()).getSchemaDefinition(any<ByteArray>())
        verify(mockGenericRecord).schema
    }

    /**
     * Test that deserializer exceptions are properly propagated.
     */
    @Test
    fun testToConnectData_DeserializerException_ThrowsDataException() {
        // Arrange
        whenever(mockDeserializer.deserialize(TEST_TOPIC, CONFLUENT_DATA))
            .thenThrow(AWSSchemaRegistryException("Deserialization failed"))

        // Act & Assert
        val exception =
            assertThrows(DataException::class.java) {
                converter.toConnectData(TEST_TOPIC, CONFLUENT_DATA)
            }
        assertTrue(
            exception.message!!.contains("Converting byte[] to Kafka Connect data failed due to serialization error"),
        )
        assertTrue(exception.cause is AWSSchemaRegistryException)
    }

    /**
     * Integration test with real GenericRecord to ensure schema extraction works end-to-end.
     */
    @Test
    fun testExtractSchemaFromAvroObject_RealGenericRecord_Success() {
        // Arrange
        val realRecord: GenericRecord = GenericData.Record(AVRO_SCHEMA)
        realRecord.put("name", "test-user")

        // Act
        val result = converter.extractSchemaFromAvroObject(realRecord)

        // Assert
        assertEquals(AVRO_SCHEMA, result)
        assertEquals("User", result.name)
        assertEquals(1, result.fields.size)
        assertEquals("name", result.fields[0].name())
    }

    /**
     * Test null value handling.
     */
    @Test
    fun testToConnectData_NullValue_ReturnsNull() {
        // Act
        val result = converter.toConnectData(TEST_TOPIC, null)

        // Assert
        assertEquals(SchemaAndValue.NULL, result)
        verifyNoInteractions(mockDeserializer, mockDeserializationFacade, mockAvroData)
    }

    companion object {
        private const val TEST_TOPIC = "test-topic"

        // 20 bytes, starts with GSR magic byte
        private val GSR_DATA =
            byteArrayOf(3, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)

        // 6 bytes, starts with Confluent magic byte
        private val CONFLUENT_DATA = byteArrayOf(0, 0, 0, 0, 1, 65)
        private const val AVRO_SCHEMA_STRING =
            """{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"""
        private val AVRO_SCHEMA: Schema = Schema.Parser().parse(AVRO_SCHEMA_STRING)
    }
}
