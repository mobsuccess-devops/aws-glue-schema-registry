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
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import com.amazonaws.services.schemaregistry.kafkastreams.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
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
 * Unit tests for testing AWSKafkaAvroSerDe class.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AWSKafkaAvroSerDeTest {
    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    private lateinit var awsKafkaAvroSerDe: AWSKafkaAvroSerDe
    private lateinit var configs: Map<String, Any>

    @BeforeEach
    fun setup() {
        configs = properties
    }

    /**
     * Test for generic record.
     */
    @Test
    fun testSerDe_GenericRecord_DeserializedEqualsSerialized() {
        val expected = RecordGenerator.createGenericAvroRecord()
        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected)

        val awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting)
        val awsKafkaAvroDeserializer = createDeserializer(expected, genericBytes)
        awsKafkaAvroSerDe = AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer)

        val genericRecord =
            awsKafkaAvroSerDe.deserializer().deserialize(
                TEST_TOPIC,
                awsKafkaAvroSerDe.serializer().serialize(TEST_TOPIC, expected),
            ) as GenericRecord

        assertEquals(expected, genericRecord)
    }

    /**
     * Test for specific record.
     */
    @Test
    fun testSerDe_SpecificRecord_DeserializedEqualsSerialized() {
        val expected = RecordGenerator.createSpecificAvroRecord()
        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(expected)

        val awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting)
        val awsKafkaAvroDeserializer = createDeserializer(expected, specificBytes)
        awsKafkaAvroSerDe = AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer)

        val user =
            awsKafkaAvroSerDe.deserializer().deserialize(
                TEST_TOPIC,
                awsKafkaAvroSerDe.serializer().serialize(TEST_TOPIC, expected),
            ) as User

        assertEquals(expected, user)
    }

    /**
     * Tests the constructor with no parameters
     */
    @Test
    fun testConstructor_noParameters_succeeds() {
        awsKafkaAvroSerDe = AWSKafkaAvroSerDe()
        assertNotNull(awsKafkaAvroSerDe)
        assertNotNull(awsKafkaAvroSerDe.serializer())
        assertNotNull(awsKafkaAvroSerDe.deserializer())
    }

    /**
     * Tests invoking close method.
     */
    @Test
    fun testClose_succeeds() {
        awsKafkaAvroSerDe = createTestAWSKafkaAvroSerDe()
        assertDoesNotThrow { awsKafkaAvroSerDe.close() }
    }

    /**
     * Test the invocation of configure method
     */
    @Test
    fun testConfigure_succeeds() {
        awsKafkaAvroSerDe = createTestAWSKafkaAvroSerDe()
        assertDoesNotThrow { awsKafkaAvroSerDe.configure(configs, false) }
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
        record: Any,
        bytes: ByteArray,
    ): AWSKafkaAvroDeserializer {
        val glueSchemaRegistryDeserializationFacade = mock<GlueSchemaRegistryDeserializationFacade>()
        val awsDeserializerInput =
            AWSDeserializerInput
                .builder()
                .buffer(ByteBuffer.wrap(bytes))
                .transportName(TEST_TOPIC)
                .build()

        whenever(glueSchemaRegistryDeserializationFacade.deserialize(awsDeserializerInput)).thenReturn(record)
        val awsKafkaAvroDeserializer = AWSKafkaAvroDeserializer(mockCredProvider, null)
        awsKafkaAvroDeserializer.configure(configs, true)

        awsKafkaAvroDeserializer.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade

        return awsKafkaAvroDeserializer
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
                AWSSchemaRegistryConstants.SCHEMA_NAME to "User-Topic",
            )

    private fun createTestAWSKafkaAvroSerDe(): AWSKafkaAvroSerDe {
        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(RecordGenerator.createGenericAvroRecord())
        val awsKafkaAvroSerializer = createSerializer(schemaDefinition, schemaVersionIdForTesting)
        val awsKafkaAvroDeserializer = createDeserializer(RecordGenerator.createGenericAvroRecord(), genericBytes)
        return AWSKafkaAvroSerDe(awsKafkaAvroSerializer, awsKafkaAvroDeserializer)
    }

    companion object {
        private const val TEST_TOPIC = "test-topic"
        private val schemaVersionIdForTesting: UUID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        private val genericBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 10, 115, 97, 110, 115, 97, 0, -58, 1, 0, 6, 114, 101, 100,
            )
        private val specificBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116,
            )
    }
}
