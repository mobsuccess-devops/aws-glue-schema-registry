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

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.utils.SchemaLoader
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import java.util.UUID

class AWSKafkaAvroSerializerTest : GlueSchemaRegistryValidationUtil() {
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher
    private val configs: MutableMap<String, Any?> = HashMap()

    private lateinit var userAvroSchema: Schema
    private lateinit var userSchemaDefinition: String
    private lateinit var employeeAvroSchema: Schema
    private lateinit var employeeSchemaDefinition: String
    private lateinit var userDefinedPojo: User
    private lateinit var genericUserAvroRecord: GenericRecord
    private lateinit var genericEmployeeAvroRecord: GenericRecord

    @BeforeEach
    fun setup() {
        mockSchemaByDefinitionFetcher = mock<SchemaByDefinitionFetcher>()

        userDefinedPojo =
            User
                .newBuilder()
                .setName("test_avros_schema")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build()
        val testTags = HashMap<String, String>()
        testTags["testKey"] = "testValue"

        userAvroSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        employeeAvroSchema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH)

        genericUserAvroRecord = RecordGenerator.createGenericAvroRecord()
        genericEmployeeAvroRecord = RecordGenerator.createGenericEmpRecord()

        userSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord)
        employeeSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericEmployeeAvroRecord)

        schemaDefinitionToSchemaVersionIdMap[userSchemaDefinition] = USER_SCHEMA_VERSION_ID
        schemaDefinitionToSchemaVersionIdMap[employeeSchemaDefinition] = EMPLOYEE_SCHEMA_VERSION_ID

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
        configs[AWSSchemaRegistryConstants.TAGS] = testTags
    }

    @Test
    fun testConfigure_schemaName_schemaNameMatches() {
        val cred = mock<AwsCredentialsProvider>()

        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        awsKafkaAvroSerializer.configure(configs, true)
        assertEquals("User-Topic", awsKafkaAvroSerializer.schemaName)
        assertNull(awsKafkaAvroSerializer.schemaNamingStrategy)
    }

    @Test
    fun testConfigure_schemaName_schemaNamingStrategyMatches() {
        val configs = HashMap<String, Any?>()

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"

        val cred = mock<AwsCredentialsProvider>()

        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        awsKafkaAvroSerializer.configure(configs, true)
        assertNotNull(awsKafkaAvroSerializer.schemaNamingStrategy)
        assertEquals(
            "com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyDefaultImpl",
            awsKafkaAvroSerializer.schemaNamingStrategy!!.javaClass.name,
        )
    }

    @Test
    fun testConfigure_customerProvidedStrategy_schemaNamingStrategyMatches() {
        val configs = HashMap<String, Any?>()

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
            "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy"

        val cred = mock<AwsCredentialsProvider>()

        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        awsKafkaAvroSerializer.configure(configs, true)
        assertNotNull(awsKafkaAvroSerializer.schemaNamingStrategy)
        assertEquals(
            "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy",
            awsKafkaAvroSerializer.schemaNamingStrategy!!.javaClass.name,
        )
    }

    @Test
    fun testConfigure_customerProvidedStrategy_throwsException() {
        val configs = HashMap<String, Any?>()

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
            "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy1"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true

        val cred = mock<AwsCredentialsProvider>()
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        val awsSchemaRegistryException =
            Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
                awsKafkaAvroSerializer.configure(configs, true)
            }

        val exceptedExceptionMessage =
            "Unable to locate the naming strategy class, check in the classpath for classname = " +
                configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS]
        assertEquals(exceptedExceptionMessage, awsSchemaRegistryException.message)
    }

    @Test
    fun testConfigure_nullConfigMapWithVersionId_throwsException() {
        val cred = mock<AwsCredentialsProvider>()

        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(cred, null)
        assertThrows(NullPointerException::class.java) { awsKafkaAvroSerializer.configure(nullOf(), true) }
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_customerProvidedStrategy_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val configs = HashMap<String, Any?>()

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
            "com.amazonaws.services.schemaregistry.serializers.avro.CustomerProvidedSchemaNamingStrategy"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val fileName = "src/test/resources/avro/user3.avsc"
        val schema = getSchema(fileName)

        val k = GenericData.EnumSymbol(schema, "ONE")
        val al = ArrayList<Int>()
        al.add(1)

        val genericRecordWithAllTypes = GenericData.Record(schema)
        val map = HashMap<String, Long>()
        map["test"] = 1L

        genericRecordWithAllTypes.put("name", "Joe")
        genericRecordWithAllTypes.put("favorite_number", 1)
        genericRecordWithAllTypes.put("meta", map)
        genericRecordWithAllTypes.put("listOfColours", al)
        genericRecordWithAllTypes.put("integerEnum", k)

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes)
        val awsKafkaAvroSerializer =
            initializeAWSKafkaAvroSerializer(
                configs,
                schemaDefinition,
                mockSchemaByDefinitionFetcher,
                USER_SCHEMA_VERSION_ID,
            )

        val schemaName =
            CustomerProvidedSchemaNamingStrategy().getSchemaName("User-Topic", genericRecordWithAllTypes, true)

        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq(schemaName),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenReturn(USER_SCHEMA_VERSION_ID)

        val serialize = awsKafkaAvroSerializer.serialize("User-Topic", genericRecordWithAllTypes)
        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType)
    }

    @Test
    fun testConstructor_defaultCredentialProvider_credentialProviderMatches() {
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer()
        assertNull(awsKafkaAvroSerializer.schemaVersionId)
        assertEquals(
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
            awsKafkaAvroSerializer.credentialProvider!!.javaClass.name,
        )
    }

    @Test
    fun testConstructor_nullCredentialProvider_succeeds() {
        assertDoesNotThrow { AWSKafkaAvroSerializer(null, USER_SCHEMA_VERSION_ID, configs) }
    }

    @Test
    fun testConstructor_configMap_succeeds() {
        assertDoesNotThrow { AWSKafkaAvroSerializer(configs) }
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(configs)
        assertNotNull(awsKafkaAvroSerializer)
    }

    @Test
    fun testConstructor_nullConfigMapWithVersionId_throwsException() {
        assertThrows(NullPointerException::class.java) {
            AWSKafkaAvroSerializer(nullOf<Map<String, Any?>>(), USER_SCHEMA_VERSION_ID)
        }
    }

    @Test
    fun testSerialize_nullData_returnsNull() {
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer()
        assertNull(awsKafkaAvroSerializer.serialize("test", null))
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_customPojos_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val awsKafkaAvroSerializer =
            initializeAWSKafkaAvroSerializer(
                configs,
                userSchemaDefinition,
                mockSchemaByDefinitionFetcher,
                USER_SCHEMA_VERSION_ID,
            )
        val serialize = awsKafkaAvroSerializer.serialize("test-topic", userDefinedPojo)

        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType)
    }

    @Test
    fun testSerialize_nullSchemaIdFromAvroSerializer_returnsNullByte() {
        val awsSerializerInput =
            AWSSerializerInput
                .builder()
                .schemaDefinition(AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord))
                .schemaName("User-Topic")
                .build()

        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer(configs, null)
        val mockGlueSchemaRegistrySerializationFacade = mock<GlueSchemaRegistrySerializationFacade>()

        awsKafkaAvroSerializer.glueSchemaRegistrySerializationFacade = mockGlueSchemaRegistrySerializationFacade
        whenever(mockGlueSchemaRegistrySerializationFacade.getOrRegisterSchemaVersion(awsSerializerInput))
            .thenReturn(null)

        assertNull(awsKafkaAvroSerializer.serialize("User-Topic", genericUserAvroRecord))
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_parseSchema_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val awsKafkaAvroSerializer =
            initializeAWSKafkaAvroSerializer(
                configs,
                userSchemaDefinition,
                mockSchemaByDefinitionFetcher,
                USER_SCHEMA_VERSION_ID,
            )
        val serialize = awsKafkaAvroSerializer.serialize("test-topic", genericUserAvroRecord)
        testForSerializedData(serialize, USER_SCHEMA_VERSION_ID, compressionType)
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_multipleRecords_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val awsKafkaAvroSerializer =
            initializeAWSKafkaAvroSerializer(
                configs,
                mockSchemaByDefinitionFetcher,
                schemaDefinitionToSchemaVersionIdMap,
            )
        val userSerializedData = awsKafkaAvroSerializer.serialize("test-topic", genericUserAvroRecord)
        testForSerializedData(userSerializedData, USER_SCHEMA_VERSION_ID, compressionType)

        val employeeSerializedData = awsKafkaAvroSerializer.serialize("test-topic", genericEmployeeAvroRecord)
        testForSerializedData(employeeSerializedData, EMPLOYEE_SCHEMA_VERSION_ID, compressionType)
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_preProvidedSchemaVersionId_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        val awsKafkaAvroSerializer = initializeAWSKafkaAvroSerializer(configs, USER_SCHEMA_VERSION_ID)
        val serializedData = awsKafkaAvroSerializer.serialize("test-topic", genericUserAvroRecord)
        testForSerializedData(serializedData, USER_SCHEMA_VERSION_ID, compressionType)
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_preProvidedSchemaVersionIdWithAnyRecord_succeeds(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        val awsKafkaAvroSerializer = initializeAWSKafkaAvroSerializer(configs, USER_SCHEMA_VERSION_ID)
        val serializedUserData = awsKafkaAvroSerializer.serialize("test-topic", genericEmployeeAvroRecord)
        testForSerializedData(serializedUserData, USER_SCHEMA_VERSION_ID, compressionType)

        // This is the validation of a case where pre-provided schemaVersionId is honored and any record will be
        // serialized with pre-provided schemaVersionId - a call to schema registry is not made by serializer
        // So - this will certainly fail while deserialization
        val employeeSerializedData = awsKafkaAvroSerializer.serialize("test-topic", genericEmployeeAvroRecord)
        testForSerializedData(employeeSerializedData, USER_SCHEMA_VERSION_ID, compressionType)
    }

    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testSerialize_sendMultipleMsgs_throwsExceptionAndSchemaVersionIdStateNotSaved(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name

        val fileName = "src/test/resources/avro/user_array_String.avsc"
        val schema = getSchema(fileName)

        val array1 = GenericData.Array<String>(1, schema)
        array1.add("1")
        val array2 = GenericData.Array<String>(1, schema)
        array1.add("2")

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(array1)
        val awsKafkaAvroSerializer =
            initializeAWSKafkaAvroSerializer(configs, schemaDefinition, mockSchemaByDefinitionFetcher, null)
        val builder =
            EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)
        val entityNotFoundException = builder.build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)
        whenever(
            mockSchemaByDefinitionFetcher.getORRegisterSchemaVersionId(
                eq(schemaDefinition),
                eq("User-Topic"),
                eq(DataFormat.AVRO.name),
                any<Map<String, String>>(),
            ),
        ).thenThrow(awsSchemaRegistryException)

        assertThrows(AWSSchemaRegistryException::class.java) {
            awsKafkaAvroSerializer.serialize("test-topic", array1)
        }
        assertThrows(AWSSchemaRegistryException::class.java) {
            awsKafkaAvroSerializer.serialize("test-topic", array2)
        }
        assertNull(awsKafkaAvroSerializer.schemaVersionId)
    }

    @Test
    fun testPrepareInput_nullDefinitionData_throwsException() {
        val awsKafkaAvroSerializer = AWSKafkaAvroSerializer()
        val method =
            AWSKafkaAvroSerializer::class.java.getDeclaredMethod(
                "prepareInput",
                Any::class.java,
                String::class.java,
                java.lang.Boolean::class.java,
            )
        method.isAccessible = true
        try {
            method.invoke(awsKafkaAvroSerializer, null, "User-Topic", true)
        } catch (e: Exception) {
            assertEquals(NullPointerException::class.java, e.cause!!.javaClass)
        }
    }

    companion object {
        private val USER_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        private val EMPLOYEE_SCHEMA_VERSION_ID = UUID.fromString("2f8e6498-29af-4722-b4ae-80f2be386bee")
        private const val AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc"
        private const val AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc"

        private val schemaDefinitionToSchemaVersionIdMap: MutableMap<String, UUID> = HashMap()
    }
}
