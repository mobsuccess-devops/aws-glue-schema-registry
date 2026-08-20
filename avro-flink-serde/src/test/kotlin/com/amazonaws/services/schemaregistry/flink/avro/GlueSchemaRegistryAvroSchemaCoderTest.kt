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

package com.amazonaws.services.schemaregistry.flink.avro

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.avro.Schema
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.doCallRealMethod
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryAvroSchemaCoderTest {
    @Mock
    private lateinit var mockClient: AWSSchemaRegistryClient

    @Mock
    private lateinit var mockCred: AwsCredentialsProvider

    @Mock
    private lateinit var mockInputStreamDeserializer: GlueSchemaRegistryInputStreamDeserializer

    private lateinit var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration

    @BeforeEach
    fun setup() {
        metadata["test-key"] = "test-value"
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TEST_TOPIC

        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = SCHEMA_NAME
        configs[AWSSchemaRegistryConstants.METADATA] = metadata
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)

        userSchema = Schema.Parser().parse(File(AVRO_USER_SCHEMA_FILE))
        userDefinedPojo =
            User
                .newBuilder()
                .setName("test_avro_schema")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build()
    }

    /**
     * Test whether constructor works
     */
    @Test
    fun testConstructor_withConfigs_succeeds() {
        assertThat(GlueSchemaRegistryAvroSchemaCoder(TEST_TOPIC, configs), notNullValue())
    }

    /**
     * Test whether readSchema method works
     */
    @Test
    fun testReadSchema_withValidParams_succeeds() {
        whenever(mockInputStreamDeserializer.getSchemaAndDeserializedStream(any<InputStream>()))
            .thenReturn(userSchema)
        val schemaCoder = GlueSchemaRegistryAvroSchemaCoder(mockInputStreamDeserializer)
        val resultSchema = schemaCoder.readSchema(buildByteArrayInputStream())

        assertThat(resultSchema, equalTo(userSchema))
    }

    /**
     * Test whether writeSchema method works
     *
     * @param compressionType compression type
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testWriteSchema_withValidParams_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        whenever(mockClient.getSchemaVersionIdByDefinition(any(), any(), any()))
            .thenReturn(USER_SCHEMA_VERSION_ID)
        val serializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .schemaByDefinitionFetcher(SchemaByDefinitionFetcher(mockClient, glueSchemaRegistryConfiguration))
                .credentialProvider(mockCred)
                .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(configs))
                .build()
        val spySerializationFacade = spy(serializationFacade)
        doCallRealMethod()
            .whenever(spySerializationFacade)
            .encode(any<String>(), any<com.amazonaws.services.schemaregistry.common.Schema>(), any())
        val outputStreamSerializer =
            GlueSchemaRegistryOutputStreamSerializer(TEST_TOPIC, configs, spySerializationFacade)
        val spyOutputStreamSerializer = spy(outputStreamSerializer)
        doCallRealMethod()
            .whenever(spyOutputStreamSerializer)
            .registerSchemaAndSerializeStream(any(), any(), any())

        val outputStream = ByteArrayOutputStream()
        outputStream.write(actualBytes)
        val schemaCoder = GlueSchemaRegistryAvroSchemaCoder(spyOutputStreamSerializer)
        schemaCoder.writeSchema(userSchema, outputStream)

        testForSerializedData(outputStream.toByteArray(), USER_SCHEMA_VERSION_ID, compressionType)
    }

    /**
     * Test whether writeSchema method throws exception if auto registration un-enabled
     */
    @Test
    fun testWriteSchema_withoutAutoRegistration_throwsException() {
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = false

        val entityNotFoundException =
            EntityNotFoundException
                .builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                .build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)
        // Override config to remove auto-registration
        glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled = false
        whenever(mockClient.getSchemaVersionIdByDefinition(any(), any(), any()))
            .thenThrow(awsSchemaRegistryException)
        configureAWSSchemaRegistryClientWithSerdeConfig(mockClient, GlueSchemaRegistryConfiguration(configs))

        val serializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .schemaByDefinitionFetcher(SchemaByDefinitionFetcher(mockClient, glueSchemaRegistryConfiguration))
                .credentialProvider(mockCred)
                .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(configs))
                .build()

        val outputStreamSerializer =
            GlueSchemaRegistryOutputStreamSerializer(TEST_TOPIC, configs, serializationFacade)
        val schemaCoder = GlueSchemaRegistryAvroSchemaCoder(outputStreamSerializer)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                schemaCoder.writeSchema(userSchema, ByteArrayOutputStream())
            }
        assertThat(exception.message, equalTo(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG))
    }

    private fun testForSerializedData(
        serializedData: ByteArray?,
        testGenericSchemaVersionId: UUID,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        assertThat(serializedData, Matchers.notNullValue())

        val buffer = getByteBuffer(serializedData!!)

        val headerVersionByte = getByte(buffer)
        val compressionByte = getByte(buffer)
        val schemaVersionId = getSchemaVersionId(buffer)

        assertThat(headerVersionByte, equalTo(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE))
        assertThat(schemaVersionId, equalTo(testGenericSchemaVersionId))

        if (AWSSchemaRegistryConstants.COMPRESSION.NONE.name == compressionType.name) {
            assertThat(compressionByte, equalTo(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE))
        } else {
            assertThat(compressionByte, equalTo(AWSSchemaRegistryConstants.COMPRESSION_BYTE))
        }
    }

    private fun buildByteArrayInputStream(): ByteArrayInputStream = ByteArrayInputStream(specificBytes)

    private fun configureAWSSchemaRegistryClientWithSerdeConfig(
        awsSchemaRegistryClient: AWSSchemaRegistryClient,
        glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
    ) {
        val serdeConfigField =
            AWSSchemaRegistryClient::class.java.getDeclaredField("glueSchemaRegistryConfiguration")
        serdeConfigField.isAccessible = true
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)
    }

    private fun getByteBuffer(bytes: ByteArray): ByteBuffer = ByteBuffer.wrap(bytes)

    private fun getByte(buffer: ByteBuffer): Byte = buffer.get()

    private fun getSchemaVersionId(buffer: ByteBuffer): UUID {
        val mostSigBits = buffer.getLong()
        val leastSigBits = buffer.getLong()
        return UUID(mostSigBits, leastSigBits)
    }

    companion object {
        private lateinit var userSchema: Schema
        private lateinit var userDefinedPojo: User
        private val configs: MutableMap<String, Any> = HashMap()
        private val metadata: MutableMap<String, String> = HashMap()

        private const val TEST_TOPIC = "Test-Topic"
        private const val SCHEMA_NAME = "User-Topic"
        private val USER_SCHEMA_VERSION_ID: UUID = UUID.randomUUID()
        private const val AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc"
        private val actualBytes =
            byteArrayOf(12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116)
        private val specificBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116,
            )
    }
}
