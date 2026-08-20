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

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.any
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair
import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.UUID

class GlueSchemaRegistrySerializationFacadeTest : GlueSchemaRegistryValidationUtil() {
    private val configs: MutableMap<String, Any?> = HashMap()
    private var schema: Schema? = null
    private lateinit var customer: Customer

    @Mock
    private lateinit var cred: AwsCredentialsProvider

    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @BeforeEach
    fun setup() {
        mockSchemaByDefinitionFetcher = mock<SchemaByDefinitionFetcher>()
        cred = mock<AwsCredentialsProvider>()
        MockitoAnnotations.openMocks(this)
        customer = Customer()
        customer.name = "test"
        val metadata = getMetadata()
        val testTags = HashMap<String, String>()
        testTags["testKey"] = "testValue"

        val parser = Schema.Parser()
        try {
            schema = parser.parse(File(AVRO_USER_SCHEMA_FILE))
        } catch (e: IOException) {
            fail<Unit>("Catch IOException: ", e)
        }

        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = USER_TOPIC
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
        configs[AWSSchemaRegistryConstants.METADATA] = metadata
        configs[AWSSchemaRegistryConstants.TAGS] = testTags
    }

    private fun createGlueSerializationFacade(
        configs: Map<String, Any?>,
        schemaByDefinitionFetcher: SchemaByDefinitionFetcher,
    ): GlueSchemaRegistrySerializationFacade = GlueSchemaRegistrySerializationFacade
        .builder()
        .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(configs))
        .credentialProvider(cred)
        .schemaByDefinitionFetcher(schemaByDefinitionFetcher)
        .build()

    /**
     * Tests serialization for generic record.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testSerialize_schemaParsing_succeeds(
        dataFormat: DataFormat,
        record: Any,
    ) {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)

        val schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record)

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq(USER_SCHEMA),
                eq(dataFormat.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING)
        val schemaVersionId =
            glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name),
            )

        assertNotNull(glueSchemaRegistrySerializationFacade.serialize(dataFormat, record, schemaVersionId))
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name)
    }

    @Test
    fun testSerialize_InvalidDataFormat_ThrowsException() {
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                glueSchemaRegistrySerializationFacade.serialize(
                    DataFormat.UNKNOWN_TO_SDK_VERSION,
                    genericAvroRecord,
                    SCHEMA_VERSION_ID_FOR_TESTING,
                )
            }
        assertTrue(exception.message!!.contains("Unsupported data format:"))
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testSerialize_NullSchemaVersionId_ThrowsException(
        dataFormat: DataFormat,
        record: Any,
    ) {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistrySerializationFacade.serialize(dataFormat, record, nullOf())
        }
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT, dataFormat.name)
    }

    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testSerialize_NullData_ThrowsException(dataFormat: DataFormat) {
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistrySerializationFacade.serialize(dataFormat, nullOf(), SCHEMA_VERSION_ID_FOR_TESTING)
        }
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade without configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    fun testBuildGSRSerializationFacade_nullConfig_throwsException() {
        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(null)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()
        }
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with null configurations but existing property.
     */
    @Test
    fun testBuildGSRSerializationFacade_nullConfigWithProp_throwsException() {
        val properties = Properties()
        properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        properties[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = USER_TOPIC

        Assertions.assertDoesNotThrow {
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(null)
                .credentialProvider(cred)
                .properties(properties)
                .build()
        }
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade without configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    fun testBuildGSRSerializationFacade_nullCredentialProvider_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(null)
                .build()
        }
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with invalid configurations will throw out
     * AWSSchemaRegistryException.
     */
    @Test
    fun testBuildGSRSerializationFacade_invalidConfigs_throwsException() {
        val configs = HashMap<String, Any?>()
        configs[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] =
            Compatibility.UNKNOWN_TO_SDK_VERSION.toString()
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = USER_TOPIC

        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()
        }
    }

    /**
     * Tests build GlueSchemaRegistrySerializationFacade with compression configuration
     */
    @ParameterizedTest
    @EnumSource(
        value = AWSSchemaRegistryConstants.COMPRESSION::class,
        names = ["NONE"],
        mode = EnumSource.Mode.EXCLUDE,
    )
    fun testBuildGSRSerializationFacade_withCompression_succeeds(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)

        assertNotNull(glueSchemaRegistrySerializationFacade)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
    }

    @Test
    fun testInitialize_nullCredentials_ThrowsException() {
        assertThrows(NullPointerException::class.java) {
            GlueSchemaRegistrySerializationFacade
                .builder()
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(configs))
                .build()
        }
    }

    /**
     * Tests serialization with null topic.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testSerialize_nullTopic_succeeds(
        dataFormat: DataFormat,
        record: Any,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name

        val schemaDefinition =
            glueSchemaRegistrySerializerFactory
                .getInstance(dataFormat, GlueSchemaRegistryConfiguration(configs))
                .getSchemaDefinition(record)

        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        val nullTopic: String? = null
        val serializedData = glueSchemaRegistryKafkaSerializer.serialize(nullTopic, record)
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialization.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testSerialize_enums_succeeds(
        dataFormat: DataFormat,
        record: Any,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name

        val schemaDefinition =
            glueSchemaRegistrySerializerFactory
                .getInstance(dataFormat, GlueSchemaRegistryConfiguration(configs))
                .getSchemaDefinition(record)
        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        val serializedData = glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record)
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialization for invalid data will throw out AWSSchemaRegistryException.
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    fun testSerialize_invalidData_throwsException(
        dataFormat: DataFormat,
        record: Any,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val schemaDefinition =
            glueSchemaRegistrySerializerFactory
                .getInstance(dataFormat, GlueSchemaRegistryConfiguration(configs))
                .getSchemaDefinition(record)
        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record)
        }
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialization for malformed JSON Schema will throw out AWSSchemaRegistryException.
     */
    @Test
    fun testSerialize_malformedJsonSchema_throwsException() {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.JSON.name
        val record = RecordGenerator.createRecordWithMalformedJsonSchema()

        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                "fakeSchemaDef",
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record)
        }
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialization for malformed JSON data will throw out AWSSchemaRegistryException.
     */
    @Test
    fun testSerialize_malformedJsonData_throwsException() {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.JSON.name
        val record = RecordGenerator.createRecordWithMalformedJsonData()

        val schemaDefinition =
            glueSchemaRegistrySerializerFactory
                .getInstance(DataFormat.JSON, GlueSchemaRegistryConfiguration(configs))
                .getSchemaDefinition(record)

        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record)
        }
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialization with unsupported protocol will throw out AWSSchemaRegistryException.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, names = ["AVRO"], mode = EnumSource.Mode.INCLUDE)
    fun testSerialize_unsupportedProtocolMessage_throwsException(dataFormat: DataFormat) {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name
        val unSupportedFormatArray = ArrayList<Int>()
        unSupportedFormatArray.add(1)

        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(configs, null, mockSchemaByDefinitionFetcher, null)

        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, unSupportedFormatArray)
        }
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests invoking shutdown invokes close method.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, names = ["UNKNOWN_TO_SDK_VERSION"], mode = EnumSource.Mode.EXCLUDE)
    fun testClose_succeeds(dataFormat: DataFormat) {
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name
        val glueSchemaRegistrySerializationFacade = mock<GlueSchemaRegistrySerializationFacade>()
        val glueSchemaRegistryKafkaSerializer =
            com.amazonaws.services.schemaregistry.serializers
                .GlueSchemaRegistryKafkaSerializer(cred, configs)
        glueSchemaRegistryKafkaSerializer.glueSchemaRegistrySerializationFacade =
            glueSchemaRegistrySerializationFacade

        glueSchemaRegistryKafkaSerializer.close()
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests serialize to check if data reduces after compression.
     */
    @ParameterizedTest
    @EnumSource(
        value = AWSSchemaRegistryConstants.COMPRESSION::class,
        names = ["NONE"],
        mode = EnumSource.Mode.EXCLUDE,
    )
    fun testSerialize_arraysWithCompression_byteArraySizeIsReduced(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val capacity = 1000000
        val schema = getSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE)
        val array = GenericData.Array<String>(capacity, schema)
        for (i in 0 until capacity) {
            array.add("test")
        }

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array)
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.AVRO.name
        val configsWithCompressionEnabled = configs.entries.associate { it.key to it.value }.toMutableMap()
        configsWithCompressionEnabled[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val gsrKafkaSerializerWithoutCompression =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )
        val gsrKafkaSerializerWithCompression =
            initializeGSRKafkaSerializer(
                configsWithCompressionEnabled,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )
        val serializedData = gsrKafkaSerializerWithoutCompression.serialize(TEST_TOPIC, array)
        val compressedAndSerializedData = gsrKafkaSerializerWithCompression.serialize(TEST_TOPIC, array)

        assertTrue(serializedData!!.size > compressedAndSerializedData!!.size)
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
    }

    /**
     * Tests registerSchemaVersion method of Serializer with metadata configuration
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    fun testSerializer_registerSchemaVersion_withMetadataConfig_succeeds(
        dataFormat: DataFormat,
        record: Any,
    ) {
        val glueSerializationFacade = createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        val schemaDefinition = glueSerializationFacade.getSchemaDefinition(dataFormat, record)
        val metadata = getMetadata()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TRANSPORT_NAME

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                schemaDefinition,
                USER_SCHEMA,
                dataFormat.name,
                metadata,
            ),
        ).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING)

        val schemaVersionId =
            glueSerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name),
            )
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId)
    }

    /**
     * Tests registerSchemaVersion method of Serializer without metadata configuration
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    fun testSerializer_registerSchemaVersion_withoutMetadataConfig_succeeds(
        dataFormat: DataFormat,
        record: Any,
    ) {
        configs.remove(AWSSchemaRegistryConstants.METADATA)
        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        val metadata = HashMap<String, String>()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TRANSPORT_NAME

        val schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record)
        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                schemaDefinition,
                USER_SCHEMA,
                dataFormat.name,
                metadata,
            ),
        ).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING)

        val schemaVersionId =
            glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name),
            )
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId)
    }

    /**
     * Tests registerSchemaVersion method of Serializer when PutSchemaVersionMetadata API throws exception
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    fun testSerializer_registerSchemaVersion_whenPutSchemaVersionMetadataThrowsException(
        dataFormat: DataFormat,
        record: Any,
    ) {
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        val awsSchemaRegistryClient = AWSSchemaRegistryClient(cred, glueSchemaRegistryConfiguration)
        val spyClient = spy(awsSchemaRegistryClient)
        val schemaByDefinitionFetcher = SchemaByDefinitionFetcher(spyClient, glueSchemaRegistryConfiguration)

        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, schemaByDefinitionFetcher)

        val schemaDefinition = glueSchemaRegistrySerializationFacade.getSchemaDefinition(dataFormat, record)

        val metadata = getMetadata()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TRANSPORT_NAME

        val entityNotFoundException =
            EntityNotFoundException
                .builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)
                .build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)
        doThrow(awsSchemaRegistryException)
            .whenever(spyClient)
            .getSchemaVersionIdByDefinition(schemaDefinition, USER_SCHEMA, dataFormat.name)

        val getSchemaVersionResponse =
            createGetSchemaVersionResponse(SCHEMA_VERSION_ID_FOR_TESTING, schemaDefinition, dataFormat.name)
        doReturn(getSchemaVersionResponse)
            .whenever(spyClient)
            .registerSchemaVersion(schemaDefinition, USER_SCHEMA, dataFormat.name)

        for (entry in metadata.entries) {
            val metadataKeyValuePair = createMetadataKeyValuePair(entry)
            doThrow(AWSSchemaRegistryException("Put schema version metadata failed."))
                .whenever(spyClient)
                .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadataKeyValuePair)
        }
        doNothing()
            .whenever(spyClient)
            .putSchemaVersionMetadata(SCHEMA_VERSION_ID_FOR_TESTING, metadata)

        val schemaVersionId =
            glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareInput(schemaDefinition, USER_SCHEMA, dataFormat.name),
            )
        assertEquals(SCHEMA_VERSION_ID_FOR_TESTING, schemaVersionId)
    }

    /**
     * Tests registerSchemaVersion method of Serializer with custom jackson configuration
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testRegisterSchemaVersion_withCustomJacksonConfiguration_succeeds(
        dataFormat: DataFormat,
        record: Any,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val jacksonSerializationFeatures = listOf(SerializationFeature.FLUSH_AFTER_WRITE_VALUE.name)
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] = jacksonSerializationFeatures
        val jacksonDeserializationFeatures = listOf(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS.name)
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] = jacksonDeserializationFeatures
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = dataFormat.name

        val schemaDefinition =
            glueSchemaRegistrySerializerFactory
                .getInstance(dataFormat, GlueSchemaRegistryConfiguration(configs))
                .getSchemaDefinition(record)
        val glueSchemaRegistryKafkaSerializer =
            initializeGSRKafkaSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                SCHEMA_VERSION_ID_FOR_TESTING,
            )

        val serializedData = glueSchemaRegistryKafkaSerializer.serialize(TEST_TOPIC, record)
        testForSerializedData(serializedData, SCHEMA_VERSION_ID_FOR_TESTING, compressionType)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
        configs.remove(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES)
        configs.remove(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES)
    }

    /**
     * Tests the encode method.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testEncode_WhenValidInputIsPassed_EncodesTheBytes(
        dataFormat: DataFormat,
        record: Any,
    ) {
        val glueSchemaRegistrySerializer =
            glueSchemaRegistrySerializerFactory.getInstance(dataFormat, GlueSchemaRegistryConfiguration(configs))

        val schemaDefinition = glueSchemaRegistrySerializer.getSchemaDefinition(record)
        val payload = glueSchemaRegistrySerializer.serialize(record)

        val schema =
            com.amazonaws.services.schemaregistry.common
                .Schema(schemaDefinition, dataFormat.name, TEST_SCHEMA)

        val metadata = getMetadata()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TRANSPORT_NAME
        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                schemaDefinition,
                TEST_SCHEMA,
                dataFormat.name,
                metadata,
            ),
        ).thenReturn(SCHEMA_VERSION_ID_FOR_TESTING)

        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)

        // Test subject
        val serializedData = glueSchemaRegistrySerializationFacade.encode(TRANSPORT_NAME, schema, payload)

        testForSerializedData(
            serializedData,
            SCHEMA_VERSION_ID_FOR_TESTING,
            AWSSchemaRegistryConstants.COMPRESSION.NONE,
            payload,
        )
    }

    @Test
    fun testEncode_WhenNonSchemaConformantDataIsPassed_ThrowsException() {
        val jsonNonSchemaConformantRecord = RecordGenerator.createNonSchemaConformantJsonData()
        val schemaDefinition = jsonNonSchemaConformantRecord.schema!!
        val payload = jsonNonSchemaConformantRecord.payload!!.toByteArray(StandardCharsets.UTF_8)

        val dataFormat = DataFormat.JSON
        val schema =
            com.amazonaws.services.schemaregistry.common
                .Schema(schemaDefinition, dataFormat.name, TEST_SCHEMA)

        val glueSchemaRegistrySerializationFacade =
            createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)

        // Test subject
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                glueSchemaRegistrySerializationFacade.encode(TRANSPORT_NAME, schema, payload)
            }
        assertEquals("JSON data validation against schema failed.", ex.message)
    }

    private fun prepareInput(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
    ): AWSSerializerInput = AWSSerializerInput
        .builder()
        .schemaDefinition(schemaDefinition)
        .schemaName(schemaName)
        .dataFormat(dataFormat)
        .build()

    @Test
    fun testRegisterSchema_nullSerializerInput_throwsException() {
        val glueSerializationFacade = createGlueSerializationFacade(configs, mockSchemaByDefinitionFetcher)
        Assertions.assertThrows(NullPointerException::class.java) {
            glueSerializationFacade.getOrRegisterSchemaVersion(nullOf())
        }
    }

    private fun getMetadata(): MutableMap<String, String> {
        val metadata = HashMap<String, String>()
        metadata["event-source-1"] = "topic1"
        metadata["event-source-2"] = "topic2"
        metadata["event-source-3"] = "topic3"
        metadata["event-source-4"] = "topic4"
        metadata["event-source-5"] = "topic5"

        return metadata
    }

    private fun createGetSchemaVersionResponse(
        schemaVersionId: UUID,
        schemaDefinition: String,
        dataFormat: String,
    ): GetSchemaVersionResponse = GetSchemaVersionResponse
        .builder()
        .schemaVersionId(schemaVersionId.toString())
        .schemaDefinition(schemaDefinition)
        .dataFormat(dataFormat)
        .build()

    private fun createMetadataKeyValuePair(metadataEntry: Map.Entry<String, String>): MetadataKeyValuePair = MetadataKeyValuePair
        .builder()
        .metadataKey(metadataEntry.key)
        .metadataValue(metadataEntry.value)
        .build()

    companion object {
        const val AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc"
        const val AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc"
        private val SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        private const val TRANSPORT_NAME = "default-stream"
        private const val TEST_SCHEMA = "test-schema"
        private const val USER_SCHEMA = "User"
        private const val TEST_TOPIC = "test-topic"
        private const val USER_TOPIC = "User-Topic"

        private val genericAvroRecord = RecordGenerator.createGenericAvroRecord()
        private val genericUserEnumAvroRecord = RecordGenerator.createGenericUserEnumAvroRecord()
        private val genericIntArrayAvroRecord = RecordGenerator.createGenericIntArrayAvroRecord()
        private val genericStringArrayAvroRecord = RecordGenerator.createGenericStringArrayAvroRecord()
        private val genericRecordInvalidEnumData = RecordGenerator.createGenericUserInvalidEnumAvroRecord()
        private val genericRecordInvalidArrayData = RecordGenerator.createGenericUserInvalidArrayAvroRecord()
        private val genericUserMapAvroRecord = RecordGenerator.createGenericUserMapAvroRecord()
        private val genericInvalidMapAvroRecord = RecordGenerator.createGenericInvalidMapAvroRecord()
        private val genericUserUnionAvroRecord = RecordGenerator.createGenericUserUnionAvroRecord()
        private val genericUserUnionNullAvroRecord = RecordGenerator.createGenericUnionWithNullValueAvroRecord()
        private val genericInvalidUnionAvroRecord = RecordGenerator.createGenericInvalidUnionAvroRecord()
        private val genericFixedAvroRecord = RecordGenerator.createGenericFixedAvroRecord()
        private val genericInvalidFixedAvroRecord = RecordGenerator.createGenericInvalidFixedAvroRecord()
        private val genericMultipleTypesAvroRecord = RecordGenerator.createGenericMultipleTypesAvroRecord()
        private val specificJsonCarRecord = RecordGenerator.createSpecificJsonRecord()
        private val invalidSpecificJsonCarRecord = RecordGenerator.createInvalidSpecificJsonRecord()
        private val specificNullCarRecord = RecordGenerator.createNullSpecificJsonRecord()
        private val userDefinedPojoAvro = RecordGenerator.createSpecificAvroRecord()
        private val glueSchemaRegistrySerializerFactory = GlueSchemaRegistrySerializerFactory()

        @JvmStatic
        fun testDataAndSchemaProvider(): List<Arguments> {
            val avroRecords: List<Any> =
                listOf(
                    genericAvroRecord,
                    genericUserEnumAvroRecord,
                    genericIntArrayAvroRecord,
                    genericStringArrayAvroRecord,
                    genericUserMapAvroRecord,
                    genericUserUnionAvroRecord,
                    genericUserUnionNullAvroRecord,
                    genericFixedAvroRecord,
                    genericMultipleTypesAvroRecord,
                    userDefinedPojoAvro,
                )

            val jsonRecords: MutableList<Any> =
                RecordGenerator.TestJsonRecord
                    .values()
                    .filter { it.isValid }
                    .map { RecordGenerator.createGenericJsonRecord(it) as Any }
                    .toMutableList()

            jsonRecords.add(specificJsonCarRecord)

            val compressions = AWSSchemaRegistryConstants.COMPRESSION.values()

            val args = ArrayList<Arguments>()

            for (compression in compressions) {
                args.addAll(avroRecords.map { Arguments.arguments(DataFormat.AVRO, it, compression) })
                args.addAll(jsonRecords.map { Arguments.arguments(DataFormat.JSON, it, compression) })
            }

            return args
        }

        @JvmStatic
        fun testInvalidDataAndSchemaProvider(): List<Arguments> {
            val avroInvalidRecords: List<Any> =
                listOf(
                    genericRecordInvalidEnumData,
                    genericRecordInvalidArrayData,
                    genericInvalidMapAvroRecord,
                    genericInvalidUnionAvroRecord,
                    genericInvalidFixedAvroRecord,
                )

            val jsonInvalidRecords: MutableList<Any> =
                RecordGenerator.TestJsonRecord
                    .values()
                    .filter { !it.isValid }
                    .map { RecordGenerator.createGenericJsonRecord(it) as Any }
                    .toMutableList()

            // Invalid JSON -> An Avro record sent instead of JSON
            jsonInvalidRecords.add(genericRecordInvalidEnumData)
            jsonInvalidRecords.add(specificNullCarRecord)
            // Invalid specific record that does not conform to schema defined by POJO
            jsonInvalidRecords.add(invalidSpecificJsonCarRecord)

            val compressions = AWSSchemaRegistryConstants.COMPRESSION.values()

            val args = ArrayList<Arguments>()

            for (compression in compressions) {
                args.addAll(avroInvalidRecords.map { Arguments.arguments(DataFormat.AVRO, it, compression) })
                args.addAll(jsonInvalidRecords.map { Arguments.arguments(DataFormat.JSON, it, compression) })
            }

            return args
        }
    }
}

class Customer {
    var name: String? = null
}
