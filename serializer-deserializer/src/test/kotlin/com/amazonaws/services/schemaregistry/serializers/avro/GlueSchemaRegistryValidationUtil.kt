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

package com.amazonaws.services.schemaregistry.serializers.avro

import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.apache.avro.Schema
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

open class GlueSchemaRegistryValidationUtil {
    /**
     * Helper function to load schema from file path
     *
     * @param fileName AVRO schema file location
     */
    protected fun getSchema(fileName: String): Schema {
        val parser = Schema.Parser()
        return parser.parse(File(fileName))
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param schemaDefinition schema definition will be used by mock client
     * @param mockSchemaByDefinitionFetcher fake schema by definition fetcher.
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     */
    protected fun initializeAWSKafkaAvroSerializer(
        configs: Map<String, Any?>,
        schemaDefinition: String?,
        mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher,
        testGenericSchemaVersionId: UUID?,
    ): AWSKafkaAvroSerializer {
        val cred = mock<AwsCredentialsProvider>()

        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()

        // Some tests stub with a null definition on purpose; the matcher has to carry that null through.
        val definitionMatcher: String = schemaDefinition ?: nullOf()

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(definitionMatcher),
                eq("User-Topic"),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(testGenericSchemaVersionId)
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        awsKafkaAvroSerializer.configure(configs, true)

        awsKafkaAvroSerializer.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade
        return awsKafkaAvroSerializer
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param mockSchemaByDefinitionFetcher fake schema by definition fetcher.
     * @param schemaDefinitionToSchemaVersionIdMap map of test schema definitions to schema version ids
     */
    protected fun initializeAWSKafkaAvroSerializer(
        configs: Map<String, Any?>,
        mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher,
        schemaDefinitionToSchemaVersionIdMap: Map<String, UUID>,
    ): AWSKafkaAvroSerializer {
        val cred = mock<AwsCredentialsProvider>()

        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()

        for (entry in schemaDefinitionToSchemaVersionIdMap.entries) {
            whenever(
                mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                    eq(entry.key),
                    eq("User-Topic"),
                    eq(DataFormat.AVRO.name),
                    any<Map<String, String>>(),
                ),
            ).thenReturn(entry.value)
        }
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        awsKafkaAvroSerializer.configure(configs, true)

        awsKafkaAvroSerializer.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade
        return awsKafkaAvroSerializer
    }

    /**
     * Test helper method to mock build serializer with pre existing schemaVersionId
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     */
    protected fun initializeAWSKafkaAvroSerializer(
        configs: Map<String, Any?>,
        testGenericSchemaVersionId: UUID,
    ): AWSKafkaAvroSerializer {
        val cred = mock<AwsCredentialsProvider>()
        val mockSchemaByDefinitionFetcher = mock<SchemaByDefinitionFetcher>()
        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(configs, testGenericSchemaVersionId)
        awsKafkaAvroSerializer.configure(configs, true)

        awsKafkaAvroSerializer.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade
        return awsKafkaAvroSerializer
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param schemaDefinition schema definition will be used by mock client
     * @param schemaByDefinitionFetcher fake schema by definition fetcher.
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     */
    protected fun initializeGSRKafkaSerializer(
        configs: Map<String, Any?>,
        schemaDefinition: String?,
        schemaByDefinitionFetcher: SchemaByDefinitionFetcher,
        testGenericSchemaVersionId: UUID?,
    ): GlueSchemaRegistryKafkaSerializer {
        val cred = mock<AwsCredentialsProvider>()

        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(schemaByDefinitionFetcher)
                .build()

        // Some tests stub with a null definition on purpose; the matcher has to carry that null through.
        val definitionMatcher: String = schemaDefinition ?: nullOf()

        whenever(
            schemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(definitionMatcher),
                eq("User-Topic"),
                eq(GlueSchemaRegistryUtils.getInstance().getDataFormat(configs)),
                any<Map<String, String>>(),
            ),
        ).thenReturn(testGenericSchemaVersionId)
        val glueSchemaRegistryKafkaSerializer = GlueSchemaRegistryKafkaSerializer(cred, null)
        glueSchemaRegistryKafkaSerializer.configure(configs, true)

        glueSchemaRegistryKafkaSerializer.glueSchemaRegistrySerializationFacade =
            glueSchemaRegistrySerializationFacade
        return glueSchemaRegistryKafkaSerializer
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param schemaByDefinitionFetcher fake schema by definition fetcher.
     * @param schemaDefinitionToSchemaVersionIdMap map of test schema definitions to schema version ids
     */
    protected fun initializeGSRKafkaSerializer(
        configs: Map<String, Any?>,
        schemaByDefinitionFetcher: SchemaByDefinitionFetcher,
        schemaDefinitionToSchemaVersionIdMap: Map<String, UUID>,
    ): GlueSchemaRegistryKafkaSerializer {
        val cred = mock<AwsCredentialsProvider>()

        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(schemaByDefinitionFetcher)
                .build()

        for (entry in schemaDefinitionToSchemaVersionIdMap.entries) {
            whenever(
                schemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                    eq(entry.key),
                    eq("User-Topic"),
                    eq(GlueSchemaRegistryUtils.getInstance().getDataFormat(configs)),
                    any<Map<String, String>>(),
                ),
            ).thenReturn(entry.value)
        }
        val glueSchemaRegistryKafkaSerializer = GlueSchemaRegistryKafkaSerializer(cred, null)
        glueSchemaRegistryKafkaSerializer.configure(configs, true)

        glueSchemaRegistryKafkaSerializer.glueSchemaRegistrySerializationFacade =
            glueSchemaRegistrySerializationFacade
        return glueSchemaRegistryKafkaSerializer
    }

    /**
     * Test helper method to mock build serializer with pre existing schemaVersionId
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     */
    protected fun initializeGSRKafkaSerializer(
        configs: Map<String, Any?>,
        testGenericSchemaVersionId: UUID,
    ): GlueSchemaRegistryKafkaSerializer {
        val cred = mock<AwsCredentialsProvider>()
        val schemaByDefinitionFetcher = mock<SchemaByDefinitionFetcher>()
        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(schemaByDefinitionFetcher)
                .build()
        val glueSchemaRegistryKafkaSerializer =
            GlueSchemaRegistryKafkaSerializer(configs, testGenericSchemaVersionId)
        glueSchemaRegistryKafkaSerializer.configure(configs, true)

        glueSchemaRegistryKafkaSerializer.glueSchemaRegistrySerializationFacade =
            glueSchemaRegistrySerializationFacade
        return glueSchemaRegistryKafkaSerializer
    }

    /**
     * Helper function to test serialized data's bytes and schemaVersionId value
     *
     * @param serializedData serialized byte array
     * @param testGenericSchemaVersionId expected schemaVersionId value
     */
    protected fun testForSerializedData(
        serializedData: ByteArray?,
        testGenericSchemaVersionId: UUID,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        testForSerializedData(serializedData, testGenericSchemaVersionId, compressionType, null)
    }

    protected fun testForSerializedData(
        serializedData: ByteArray?,
        testGenericSchemaVersionId: UUID,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
        expectedPayload: ByteArray?,
    ) {
        assertNotNull(serializedData)

        val buffer = getByteBuffer(serializedData!!)

        val headerVersionByte = getByte(buffer)
        val compressionByte = getByte(buffer)
        val schemaVersionId = getSchemaVersionId(buffer)

        assertEquals(3.toByte(), headerVersionByte)
        assertEquals(testGenericSchemaVersionId, schemaVersionId)

        if (compressionType.name == AWSSchemaRegistryConstants.COMPRESSION.NONE.name) {
            assertEquals(0.toByte(), compressionByte)
        } else {
            assertEquals(5.toByte(), compressionByte)
        }

        if (expectedPayload != null) {
            val actualPayload = ByteArray(buffer.remaining())
            buffer.get(actualPayload)
            assertArrayEquals(expectedPayload, actualPayload)
        }
    }

    private fun getByteBuffer(bytes: ByteArray): ByteBuffer = ByteBuffer.wrap(bytes)

    private fun getByte(buffer: ByteBuffer): Byte = buffer.get()

    private fun getSchemaVersionId(buffer: ByteBuffer): UUID {
        val mostSigBits = buffer.long
        val leastSigBits = buffer.long
        return UUID(mostSigBits, leastSigBits)
    }
}
