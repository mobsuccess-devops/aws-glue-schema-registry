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

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.json.Car
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.utils.SchemaLoader
import com.amazonaws.services.schemaregistry.utils.SerializedByteArrayGenerator
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Properties
import java.util.UUID

/**
 * Unit tests for testing protocol agnostic de-serializer.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GlueSchemaRegistryDeserializationFacadeTest {
    private val configs: MutableMap<String, Any?> = HashMap()

    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    @Mock
    private lateinit var mockDefaultRegistryClient: AWSSchemaRegistryClient

    @Mock
    private lateinit var mockClientThatThrowsException: AWSSchemaRegistryClient

    @Mock
    private lateinit var mockSchemaRegistryClient: AWSSchemaRegistryClient

    @Mock
    private lateinit var mockDefaultCredProvider: AwsCredentialsProvider

    @Mock
    private lateinit var mockDataFormatDeserializer: GlueSchemaRegistryDataFormatDeserializer

    @Mock
    private lateinit var mockDeserializerFactory: GlueSchemaRegistryDeserializerFactory

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    fun setup() {
        this.configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        this.configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true

        whenever(mockClientThatThrowsException.getSchemaVersionResponse(any<String>()))
            .thenThrow(AWSSchemaRegistryException("some runtime exception"))

        glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build()

        val compressionConfig = HashMap<String, Any?>()
        compressionConfig.putAll(this.configs)
        compressionConfig[AWSSchemaRegistryConstants.COMPRESSION_TYPE] =
            AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString()

        compressingGlueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(compressionConfig)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build()

        genericUserAvroRecord = RecordGenerator.createGenericAvroRecord()
        userAvroSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        userSchemaDefinition = AVRO_UTILS.getSchemaDefinition(genericUserAvroRecord)
        userSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(userAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(USER_SCHEMA_ARN)
                .build()

        genericEmployeeAvroRecord = RecordGenerator.createGenericEmpRecord()
        employeeAvroSchema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH)
        employeeSchemaDefinition = AVRO_UTILS.getSchemaDefinition(genericEmployeeAvroRecord)
        employeeSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(employeeAvroSchema.toString())
                .dataFormat(DataFormat.AVRO)
                .schemaArn(EMPLOYEE_SCHEMA_ARN)
                .build()

        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(USER_SCHEMA_VERSION_ID.toString())))
            .thenReturn(userSchemaVersionResponse)
        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(EMPLOYEE_SCHEMA_VERSION_ID.toString())))
            .thenReturn(employeeSchemaVersionResponse)

        whenever(
            mockDataFormatDeserializer.deserialize(
                any<ByteBuffer>(),
                eq(Schema(employeeAvroSchema.toString(), DataFormat.AVRO.name, "employee_schema")),
            ),
        ).thenReturn(genericEmployeeAvroRecord)
        whenever(
            mockDataFormatDeserializer.deserialize(
                any<ByteBuffer>(),
                eq(Schema(userAvroSchema.toString(), DataFormat.AVRO.name, "user_schema")),
            ),
        ).thenReturn(genericUserAvroRecord)

        whenever(
            mockDeserializerFactory.getInstance(
                any<DataFormat>(),
                any<GlueSchemaRegistryConfiguration>(),
            ),
        ).thenReturn(mockDataFormatDeserializer)

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(userSchemaDefinition),
                eq(USER_SCHEMA_NAME),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(USER_SCHEMA_VERSION_ID)
        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(employeeSchemaDefinition),
                eq(EMPLOYEE_SCHEMA_NAME),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(EMPLOYEE_SCHEMA_VERSION_ID)
    }

    /**
     * Clears the className resolution opt-in from the shared configs map. Runs even when a test
     * fails partway through, so an aborted test cannot leak the opt-in into later tests.
     */
    @AfterEach
    fun disableJsonClassNameResolution() {
        configs.remove(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED)
        configs.remove(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST)
    }

    /**
     * Tests the GlueSchemaRegistryemployeeDeserializationFacade instantiation when an no configuration is provided.
     */
    @Test
    fun testBuildDeserializer_withNoArguments_throwsException() {
        assertThrows(NullPointerException::class.java) {
            GlueSchemaRegistryDeserializationFacade.builder().build()
        }
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an no configuration is provided.
     */
    @Test
    fun testBuildDeserializer_withNullConfig_throwsException() {
        assertThrows(NullPointerException::class.java) {
            GlueSchemaRegistryDeserializationFacade(nullOf(), DefaultCredentialsProvider.create())
        }
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade and assert for dependency values with config map.
     */
    @Test
    fun testBuildDeserializer_withConfigs_buildsSuccessfully() {
        val glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .configs(this.configs)
                .build()

        assertEquals(this.mockDefaultCredProvider, glueSchemaRegistryDeserializationFacade.credentialsProvider)
        assertEquals(
            GlueSchemaRegistryConfiguration(this.configs),
            glueSchemaRegistryDeserializationFacade.glueSchemaRegistryConfiguration,
        )
    }

    /**
     * Tests that overriding the user-agent app name accepts a null, which the Java source stored
     * as-is.
     */
    @Test
    fun testOverrideUserAgentApp_withNull_storesNull() {
        val glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .configs(this.configs)
                .build()

        glueSchemaRegistryDeserializationFacade.overrideUserAgentApp("kafkaconnect")
        assertEquals(
            "kafkaconnect",
            glueSchemaRegistryDeserializationFacade.glueSchemaRegistryConfiguration.userAgentApp,
        )

        glueSchemaRegistryDeserializationFacade.overrideUserAgentApp(null)
        assertNull(glueSchemaRegistryDeserializationFacade.glueSchemaRegistryConfiguration.userAgentApp)
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade and assert for dependency values with properties.
     */
    @Test
    fun testBuildDeserializer_withProperties_buildsSuccessfully() {
        val props = Properties()
        props[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"

        val glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .properties(props)
                .build()

        assertEquals(this.mockDefaultCredProvider, glueSchemaRegistryDeserializationFacade.credentialsProvider)
        assertEquals(this.mockDefaultRegistryClient, glueSchemaRegistryDeserializationFacade.schemaRegistryClient)
        assertEquals(
            GlueSchemaRegistryConfiguration(props),
            glueSchemaRegistryDeserializationFacade.glueSchemaRegistryConfiguration,
        )
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an empty configuration is provided.
     */
    @Test
    fun testBuildDeserializer_emptyConfig_throwsException() {
        assertThrows(IllegalArgumentException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(HashMap<String, Any?>())
                .build()
        }
        assertThrows(IllegalArgumentException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .properties(Properties())
                .build()
        }
        assertThrows(IllegalArgumentException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(HashMap<String, Any?>())
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .build()
        }
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an invalid configuration is provided.
     */
    @Test
    fun testBuildDeserializer_badConfig_throwsException() {
        val badConfig: Map<String, Any?> =
            hashMapOf(
                AWSSchemaRegistryConstants.COMPATIBILITY_SETTING to
                    Compatibility.UNKNOWN_TO_SDK_VERSION.toString(),
            )
        assertThrows(AWSSchemaRegistryException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(badConfig)
                .build()
        }
        assertThrows(AWSSchemaRegistryException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(badConfig)
                .schemaRegistryClient(this.mockDefaultRegistryClient)
                .build()
        }
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade instantiation when an null configuration is provided.
     */
    @Test
    fun testBuildDeserializer_nullConfig_throwsException() {
        assertThrows(AWSSchemaRegistryException::class.java) {
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(null)
                .build()
        }
        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                GlueSchemaRegistryDeserializationFacade
                    .builder()
                    .credentialProvider(this.mockDefaultCredProvider)
                    .configs(null)
                    .schemaRegistryClient(this.mockDefaultRegistryClient)
                    .build()
            }

        assertEquals("Either properties or configuration has to be provided", exception.message)
    }

    /**
     * Tests the GlueSchemaRegistryDeserializationFacade by retrieving schema definition - positive case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testGetSchemaDefinition_getSchemaVersionFromClientSucceeds_schemaDefinitionMatches(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
        avroRecordType: String,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)

        val schemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)
        val schemaDefinition = glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData)
        assertEquals(inputSchemaDefinition, schemaDefinition)
        // Clean-up
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
    }

    /**
     * Tests the DeserializationFacade by retrieving schema definition - negative case.
     * Tests the GlueSchemaRegistryDeserializationFacade by retrieving schema definition - negative case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testGetSchemaDefinition_getSchemaVersionFromClientFails_throwsException(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
    ) {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockClientThatThrowsException)
        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)
        assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData)
        }
    }

    /**
     * Tests the getSchemaVersionId for exception case where data length is invalid.
     */
    @Test
    fun testGetSchemaDefinition_invalidDataLength_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        val serializedData =
            byteArrayOf(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
            )
        assertThrows(GlueSchemaRegistryIncompatibleDataException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData)
        }
    }

    /**
     * Tests the getSchemaVersionId for exception case where the header version byte is unknown.
     */
    @Test
    fun testGetSchemaDefinition_invalidHeaderVersionByte_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        val serializedData =
            SerializedByteArrayGenerator.constructBasicSerializedData(
                99,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                UUID.randomUUID(),
            )
        assertThrows(GlueSchemaRegistryIncompatibleDataException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData)
        }
    }

    /**
     * Tests the getSchemaVersionId for exception case where the compression byte is unknown.
     */
    @Test
    fun testGetSchemaDefinition_invalidCompressionByte_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        val serializedData =
            SerializedByteArrayGenerator.constructBasicSerializedData(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                99,
                UUID.randomUUID(),
            )

        assertThrows(GlueSchemaRegistryIncompatibleDataException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(serializedData)
        }
    }

    /**
     * Tests the getSchemaVersionId for exception case where the buffer is null.
     */
    @Test
    fun testGetSchemaDefinition_nullBuffer_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(nullOf<ByteBuffer>())
        }
    }

    /**
     * Tests the getSchemaVersionId for exception case where the byte array is null.
     */
    @Test
    fun testGetSchemaDefinition_nullByte_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryDeserializationFacade.getSchemaDefinition(nullOf<ByteArray>())
        }
    }

    /**
     * Tests the de-serialization positive case.
     */
    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testDeserialize_withValidSchemaResponse_recordMatches(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
        avroRecordType: String,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        configs[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = avroRecordType
        // The JSON specific record in this provider carries a className, so opt in to className
        // resolution and allow that class in order to get a typed POJO back.
        enableJsonClassNameResolution(CAR_CLASS_NAME)
        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)

        val schemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)

        val deserializedObject =
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))

        // AVRO converts strings into org.apache.avro.util.Utf8 char sequences and equals of it does not work
        // with java.lang.String
        // hence using toString() so that equality checks for avro too pass
        // https://stackoverflow.com/questions/15690997/avro-and-java-deserialized-map-of-string-doesnt-equals
        // -original-map
        assertEquals(record.toString(), deserializedObject.toString())

        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testDeserialize_withSerdeConfigs_recordMatches(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
        avroRecordType: String,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        configs[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = avroRecordType
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            listOf(DeserializationFeature.EAGER_DESERIALIZER_FETCH.name)
        // The JSON specific record in this provider carries a className, so opt in to className
        // resolution and allow that class in order to get a typed POJO back.
        enableJsonClassNameResolution(CAR_CLASS_NAME)
        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)

        val schemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)

        val deserializedObject =
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))

        // AVRO converts strings into org.apache.avro.util.Utf8 char sequences and equals of it does not work
        // with java.lang.String
        // hence using toString() so that equality checks for avro too pass
        // https://stackoverflow.com/questions/15690997/avro-and-java-deserialized-map-of-string-doesnt-equals
        // -original-map
        assertEquals(record.toString(), deserializedObject.toString())

        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE)
        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES)
    }

    /**
     * Tests the de-serialization of specific json record with wrong classname
     */
    @ParameterizedTest
    @MethodSource("testInvalidDataAndSchemaProvider")
    fun testDeserialize_invalidSpecificJsonRecord_throwsException(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
        avroRecordType: String,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        // Allowlist the (non-existent) class named by the schema so that resolution is actually
        // attempted; the failure under test is Class.forName, not the allowlist check.
        enableJsonClassNameResolution(INVALID_CLASS_NAME)

        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)

        val schemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)

        assertThrows(AWSSchemaRegistryException::class.java) {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
    }

    /**
     * Tests the de-serialization of multiple records of different schemas.
     */
    @ParameterizedTest
    @EnumSource(
        value = DataFormat::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["UNKNOWN_TO_SDK_VERSION", "JSON", "PROTOBUF"],
    )
    fun testDeserialize_withMultipleRecords_recordsMatch(dataFormat: DataFormat) {
        val serializedUserData = createSerializedUserData(genericUserAvroRecord, dataFormat)
        val serializedEmployeeData = createSerializedEmployeeData(genericEmployeeAvroRecord, dataFormat)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockDeserializerFactory)
        val deserializedUserObject =
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedUserData))
        val deserializedEmployeeObject =
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedEmployeeData))

        assertEquals(genericUserAvroRecord, deserializedUserObject)
        assertEquals(genericEmployeeAvroRecord, deserializedEmployeeObject)
    }

    /**
     * Tests the de-serialization negative case UnknownDataException.
     */
    @Test
    fun testDeserialize_invalidData_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        val serializedData =
            byteArrayOf(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
            )
        assertThrows(GlueSchemaRegistryIncompatibleDataException::class.java) {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }
    }

    @ParameterizedTest
    @MethodSource("testDataAndSchemaProvider")
    fun testGetActualData_withValidBytes_ReturnsActualBytes(
        dataFormat: DataFormat,
        record: Any,
        inputSchemaDefinition: String,
        schemaVersionId: UUID,
        avroRecordType: String,
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
        bytes: ByteArray,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val serializedData = createSerializedData(record, dataFormat, inputSchemaDefinition, schemaVersionId)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)

        val actualBytes = glueSchemaRegistryDeserializationFacade.getActualData(serializedData)

        // Convert to ByteBuffer and compare.
        assertEquals(ByteBuffer.wrap(bytes), ByteBuffer.wrap(actualBytes))

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
    }

    /**
     * Tests the de-serialization for exception case where the deserializer input is null.
     */
    @Test
    fun testDeserialize_nullDeserializerInput_throwsException() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryDeserializationFacade.deserialize(nullOf<AWSDeserializerInput>())
        }
    }

    /**
     * Tests the deserialization case where retrieved schema data is stored in cache
     */
    @Test
    fun testDeserializer_retrieveSchemaRegistryMetadata_MetadataIsCached() {
        val dataFormat = DataFormat.AVRO.name
        val inputSchemaDefinition = userSchemaDefinition
        val schemaVersionId = USER_SCHEMA_VERSION_ID

        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = AWSSchemaRegistryConstants.COMPRESSION.NONE.name
        configs[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.name

        val serializedData =
            createSerializedData(
                genericUserAvroRecord,
                DataFormat.valueOf(dataFormat),
                inputSchemaDefinition,
                schemaVersionId,
            )

        val schemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaDefinition(inputSchemaDefinition)
                .dataFormat(dataFormat)
                .schemaArn(TEST_SCHEMA_ARN)
                .build()

        // Mock to return success and failures.
        whenever(mockSchemaRegistryClient.getSchemaVersionResponse(eq(schemaVersionId.toString())))
            .thenReturn(schemaVersionResponse)
            .thenReturn(schemaVersionResponse)
            .thenThrow(RuntimeException("Service outage"))
            .thenReturn(schemaVersionResponse)

        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade(mockSchemaRegistryClient)

        val cache = glueSchemaRegistryDeserializationFacade.cache

        // Make sure cache is empty to start with.
        assertEquals(0, cache.size())

        assertDoesNotThrow {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }

        // Ensure cache only one value as desired.
        assertEquals(1, cache.size())

        @Suppress("UNCHECKED_CAST")
        val cacheEntry =
            cache
                .asMap()
                .entries
                .toTypedArray()[0] as Map.Entry<UUID, Schema>
        val expectedSchema = Schema(inputSchemaDefinition, dataFormat, "test_schema")

        // Verify cache contents.
        assertEquals(schemaVersionId, cacheEntry.key)
        assertEquals(expectedSchema, cacheEntry.value)

        // Expire cache.
        cache.refresh(schemaVersionId)

        // Failed service call shouldn't result in exceptions.
        assertDoesNotThrow {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }
        assertEquals(1, cache.size())

        // Subsequent calls shouldn't fail either.
        assertDoesNotThrow {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }
        assertDoesNotThrow {
            glueSchemaRegistryDeserializationFacade.deserialize(prepareDeserializerInput(serializedData))
        }

        verify(mockSchemaRegistryClient, times(2)).getSchemaVersionResponse(eq(schemaVersionId.toString()))

        configs.remove(AWSSchemaRegistryConstants.COMPRESSION_TYPE)
        configs.remove(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE)
    }

    /**
     * Tests invoking close method.
     */
    @Test
    fun testClose_succeeds() {
        val glueSchemaRegistryDeserializationFacade = createGSRDeserializationFacade()
        assertDoesNotThrow { glueSchemaRegistryDeserializationFacade.close() }
    }

    @ParameterizedTest
    @EnumSource(
        value = DataFormat::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["UNKNOWN_TO_SDK_VERSION", "JSON", "PROTOBUF"],
    )
    fun testCanDeserialize_WhenValidBytesArePassed_ReturnsTrue(dataFormat: DataFormat) {
        val validSchemaRegistryBytes = createSerializedCompressedEmployeeData(genericEmployeeAvroRecord, dataFormat)
        assertTrue(createGSRDeserializationFacade().canDeserialize(validSchemaRegistryBytes))
    }

    @Test
    fun testCanDeserialize_WhenNullBytesArePassed_ReturnsFalse() {
        assertFalse(createGSRDeserializationFacade().canDeserialize(null))
    }

    @Test
    fun testCanDeserialize_WhenInvalidBytesArePassed_ReturnsFalse() {
        assertFalse(createGSRDeserializationFacade().canDeserialize(byteArrayOf(9, 2, 1)))
    }

    /**
     * Helper method to serialize data for testing de-serialization.
     */
    private fun createSerializedData(
        objectToSerialize: Any,
        dataFormat: DataFormat,
        schemaDefinition: String,
        schemaVersionId: UUID,
    ): ByteArray {
        val serializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
                .build()
        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq(TEST_SCHEMA_NAME),
                eq(dataFormat.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(schemaVersionId)
        return serializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId)
    }

    /**
     * Helper method to serialize USER data for testing de-serialization.
     */
    private fun createSerializedUserData(
        objectToSerialize: Any,
        dataFormat: DataFormat,
    ): ByteArray {
        val schemaVersionId =
            glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(userSchemaDefinition, USER_SCHEMA_NAME, dataFormat.name),
            )
        return glueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId)
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private fun createSerializedEmployeeData(
        objectToSerialize: Any,
        dataFormat: DataFormat,
    ): ByteArray {
        val schemaVersionId =
            glueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME, dataFormat.name),
            )
        return glueSchemaRegistrySerializationFacade.serialize(dataFormat, objectToSerialize, schemaVersionId)
    }

    /**
     * Helper method to serialize EMPLOYEE data for testing de-serialization.
     */
    private fun createSerializedCompressedEmployeeData(
        objectToSerialize: Any,
        dataFormat: DataFormat,
    ): ByteArray {
        val schemaVersionId =
            compressingGlueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(
                prepareSerializerInput(employeeSchemaDefinition, EMPLOYEE_SCHEMA_NAME, dataFormat.name),
            )
        return compressingGlueSchemaRegistrySerializationFacade.serialize(
            dataFormat,
            objectToSerialize,
            schemaVersionId,
        )
    }

    /**
     * Opts in to className-based JSON deserialization and allows the given class, so that a schema
     * carrying a `className` is deserialized into that POJO rather than a JsonDataWithSchema.
     *
     * @param allowedClassName fully qualified class name to add to the allowlist
     */
    private fun enableJsonClassNameResolution(allowedClassName: String) {
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = true
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = allowedClassName
    }

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private fun createGSRDeserializationFacade(): GlueSchemaRegistryDeserializationFacade = GlueSchemaRegistryDeserializationFacade
        .builder()
        .credentialProvider(this.mockDefaultCredProvider)
        .configs(this.configs)
        .schemaRegistryClient(this.mockDefaultRegistryClient)
        .build()

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @param mockClient schema registry mock client
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private fun createGSRDeserializationFacade(
        mockClient: AWSSchemaRegistryClient,
    ): GlueSchemaRegistryDeserializationFacade = GlueSchemaRegistryDeserializationFacade
        .builder()
        .credentialProvider(this.mockDefaultCredProvider)
        .configs(this.configs)
        .schemaRegistryClient(mockClient)
        .build()

    /**
     * Helper method to create GlueSchemaRegistryDeserializationFacade instance.
     *
     * @param glueSchemaRegistryDeserializerFactory de-serializer factory instance
     * @return GlueSchemaRegistryDeserializationFacade instance.
     */
    private fun createGSRDeserializationFacade(
        glueSchemaRegistryDeserializerFactory: GlueSchemaRegistryDeserializerFactory,
    ): GlueSchemaRegistryDeserializationFacade {
        val glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(this.configs)
                .schemaRegistryClient(mockSchemaRegistryClient)
                .build()

        glueSchemaRegistryDeserializationFacade.deserializerFactory = glueSchemaRegistryDeserializerFactory

        return glueSchemaRegistryDeserializationFacade
    }

    private fun prepareSerializerInput(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
    ): AWSSerializerInput = AWSSerializerInput
        .builder()
        .schemaDefinition(schemaDefinition)
        .schemaName(schemaName)
        .dataFormat(dataFormat)
        .build()

    private fun prepareDeserializerInput(data: ByteArray): AWSDeserializerInput = AWSDeserializerInput
        .builder()
        .buffer(ByteBuffer.wrap(data))
        .build()

    companion object {
        const val AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc"
        const val AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc"
        private const val TEST_SCHEMA_NAME = "Test"
        private const val TEST_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name/test_schema"
        private const val USER_SCHEMA_NAME = "User"
        private val USER_SCHEMA_VERSION_ID = UUID.randomUUID()
        private const val USER_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name/user_schema"
        private const val EMPLOYEE_SCHEMA_NAME = "Employee"
        private val EMPLOYEE_SCHEMA_VERSION_ID = UUID.randomUUID()
        private const val EMPLOYEE_SCHEMA_ARN =
            "arn:aws:glue:ca-central-1:111111111111:schema/registry_name/employee_schema"

        private val AVRO_UTILS = AVROUtils.getInstance()
        private val genericAvroRecord = RecordGenerator.createGenericAvroRecord()
        private val genericUserEnumAvroRecord = RecordGenerator.createGenericUserEnumAvroRecord()
        private val genericIntArrayAvroRecord = RecordGenerator.createGenericIntArrayAvroRecord()
        private val genericStringArrayAvroRecord = RecordGenerator.createGenericStringArrayAvroRecord()
        private val genericUserMapAvroRecord = RecordGenerator.createGenericUserMapAvroRecord()
        private val genericUserUnionAvroRecord = RecordGenerator.createGenericUserUnionAvroRecord()
        private val genericUserUnionNullAvroRecord = RecordGenerator.createGenericUnionWithNullValueAvroRecord()
        private val genericFixedAvroRecord = RecordGenerator.createGenericFixedAvroRecord()
        private val genericMultipleTypesAvroRecord = RecordGenerator.createGenericMultipleTypesAvroRecord()
        private val specificJsonCarRecord = RecordGenerator.createSpecificJsonRecord()
        private val specificJsonEmpployeeRecord = RecordGenerator.createInvalidEmployeeJsonRecord()
        private val CAR_CLASS_NAME = Car::class.java.name

        /** The className injected into [Employee]'s schema, which intentionally does not resolve. */
        private const val INVALID_CLASS_NAME = "wrong.class.name"
        private val userDefinedPojoAvro = RecordGenerator.createSpecificAvroRecord()
        private val glueSchemaRegistryConfiguration =
            GlueSchemaRegistryConfiguration(
                hashMapOf<String, Any?>(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2"),
            )
        private val JSON_SERIALIZER = JsonSerializer(glueSchemaRegistryConfiguration)

        private lateinit var genericUserAvroRecord: GenericRecord
        private lateinit var userAvroSchema: org.apache.avro.Schema
        private lateinit var userSchemaDefinition: String
        private lateinit var userSchemaVersionResponse: GetSchemaVersionResponse
        private lateinit var genericEmployeeAvroRecord: GenericRecord
        private lateinit var employeeAvroSchema: org.apache.avro.Schema
        private lateinit var employeeSchemaDefinition: String
        private lateinit var employeeSchemaVersionResponse: GetSchemaVersionResponse
        private lateinit var glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade
        private lateinit var compressingGlueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade

        @JvmStatic
        fun testDataAndSchemaProvider(): List<Arguments> {
            val genericAvroRecords: List<Any> =
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
                )

            val specificAvroRecords: List<Any> = listOf(userDefinedPojoAvro)

            val wrapperJsonRecords: List<Any> =
                RecordGenerator.TestJsonRecord
                    .values()
                    .filter { it.isValid }
                    .map { RecordGenerator.createGenericJsonRecord(it) as Any }

            val specificJsonRecords: List<Any> = listOf(specificJsonCarRecord)

            val compressions = AWSSchemaRegistryConstants.COMPRESSION.values()

            val args = ArrayList<Arguments>()

            for (compression in compressions) {
                args.addAll(
                    genericAvroRecords.map { r ->
                        Arguments.arguments(
                            DataFormat.AVRO,
                            r,
                            AVRO_UTILS.getSchemaDefinition(r),
                            UUID.randomUUID(),
                            AvroRecordType.GENERIC_RECORD.getName(),
                            compression,
                            getAvroBytes(r, AVRO_UTILS.getSchemaDefinition(r)),
                        )
                    },
                )
                args.addAll(
                    specificAvroRecords.map { r ->
                        Arguments.arguments(
                            DataFormat.AVRO,
                            r,
                            AVRO_UTILS.getSchemaDefinition(r),
                            UUID.randomUUID(),
                            AvroRecordType.SPECIFIC_RECORD.getName(),
                            compression,
                            getAvroBytes(r, AVRO_UTILS.getSchemaDefinition(r)),
                        )
                    },
                )
                args.addAll(
                    wrapperJsonRecords.map { r ->
                        Arguments.arguments(
                            DataFormat.JSON,
                            r,
                            JSON_SERIALIZER.getSchemaDefinition(r),
                            UUID.randomUUID(),
                            // Not being used
                            AvroRecordType.GENERIC_RECORD.getName(),
                            compression,
                            JSON_SERIALIZER.serialize(r),
                        )
                    },
                )
                args.addAll(
                    specificJsonRecords.map { r ->
                        Arguments.arguments(
                            DataFormat.JSON,
                            r,
                            JSON_SERIALIZER.getSchemaDefinition(r),
                            UUID.randomUUID(),
                            // Not being used
                            AvroRecordType.GENERIC_RECORD.getName(),
                            compression,
                            JSON_SERIALIZER.serialize(r),
                        )
                    },
                )
            }

            return args
        }

        @JvmStatic
        fun testInvalidDataAndSchemaProvider(): List<Arguments> {
            val specificJsonRecords: List<Any> = listOf(specificJsonEmpployeeRecord)

            val compressions = AWSSchemaRegistryConstants.COMPRESSION.values()

            val args = ArrayList<Arguments>()

            for (compression in compressions) {
                args.addAll(
                    specificJsonRecords.map { r ->
                        Arguments.arguments(
                            DataFormat.JSON,
                            r,
                            JSON_SERIALIZER.getSchemaDefinition(r),
                            UUID.randomUUID(),
                            // Not being used
                            AvroRecordType.GENERIC_RECORD.getName(),
                            compression,
                            JSON_SERIALIZER.serialize(r),
                        )
                    },
                )
            }

            return args
        }

        private fun getAvroBytes(
            record: Any,
            inputSchemaDefinition: String,
        ): ByteArray {
            val expectedBytes = ByteArrayOutputStream()
            val writer = GenericDatumWriter<Any>(AVRO_UTILS.parseSchema(inputSchemaDefinition))
            val encoder = EncoderFactory.get().directBinaryEncoder(expectedBytes, null)
            try {
                writer.write(record, encoder)
                encoder.flush()
            } catch (e: IOException) {
                fail<Unit>("Unable to get bytes from record.")
            }
            return expectedBytes.toByteArray()
        }
    }
}
