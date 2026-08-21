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

package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.GetTagsRequest
import software.amazon.awssdk.services.glue.model.GetTagsResponse
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.RegistryId
import software.amazon.awssdk.services.glue.model.SchemaId
import java.io.File
import java.io.IOException
import java.net.URISyntaxException
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class AWSSchemaRegistryClientTest {
    @Mock
    private var mockGlueClient: GlueClient? = null

    private val configs: MutableMap<String, Any> = HashMap()
    private lateinit var awsSchemaRegistryClient: AWSSchemaRegistryClient
    private lateinit var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration
    private lateinit var userSchemaDefinition: String
    private lateinit var genericUserAvroRecord: GenericRecord
    private var schema: Schema? = null
    private lateinit var testTags: MutableMap<String, String>

    @BeforeEach
    fun setup() {
        awsSchemaRegistryClient = AWSSchemaRegistryClient(mockGlueClient!!)

        val parser = Schema.Parser()
        try {
            schema = parser.parse(File(AVRO_USER_SCHEMA_FILE))
        } catch (e: IOException) {
            fail<Unit>("Catch IOException: ", e)
        }

        genericUserAvroRecord = GenericData.Record(schema)
        genericUserAvroRecord.put("name", "sansa")
        genericUserAvroRecord.put("favorite_number", 99)
        genericUserAvroRecord.put("favorite_color", "red")
        testTags = HashMap()
        testTags["testKey"] = "testValue"

        userSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord)

        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.REGISTRY_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.TAGS] = testTags
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
    }

    @Test
    fun testAWSSchemaRegistryClient_putSchemaVersionMetadata_succeeds() {
        val metadata = getMetadata()

        for (entry in metadata.entries) {
            val metadataKeyValuePair = createMetadataKeyValuePair(entry)
            val putSchemaVersionMetadataRequest =
                createPutSchemaVersionMetadataRequest(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair)
            val putSchemaVersionMetadataResponse =
                createPutSchemaVersionMetadataResponse(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair)
            whenever(mockGlueClient!!.putSchemaVersionMetadata(putSchemaVersionMetadataRequest))
                .thenReturn(putSchemaVersionMetadataResponse)
        }

        awsSchemaRegistryClient.putSchemaVersionMetadata(SCHEMA_ID_FOR_TESTING, metadata)
        for (entry in metadata.entries) {
            val metadataKeyValuePair = createMetadataKeyValuePair(entry)
            val putSchemaVersionMetadataRequest =
                createPutSchemaVersionMetadataRequest(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair)
            verify(mockGlueClient!!, times(1)).putSchemaVersionMetadata(putSchemaVersionMetadataRequest)
        }
    }

    @Test
    fun testConstructor_nullCredentials_throwsException() {
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        Assertions.assertThrows(NullPointerException::class.java) {
            AWSSchemaRegistryClient(nullOf(), glueSchemaRegistryConfiguration)
        }
    }

    @Test
    fun testConstructor_nullSerdeConfigs_throwsException() {
        val mockAwsCredentialsProvider = mock<AwsCredentialsProvider>()
        Assertions.assertThrows(NullPointerException::class.java) {
            AWSSchemaRegistryClient(mockAwsCredentialsProvider, nullOf())
        }
    }

    @Test
    fun testConstructor_nullGlueClient_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            AWSSchemaRegistryClient(nullOf<GlueClient>())
        }
    }

    @Test
    fun testConstructor_withMalformedUri_throwsException() {
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        val invalidURL = "://abc:com"
        glueSchemaRegistryConfiguration.endPoint = invalidURL
        val mockAwsCredentialsProvider = mock<AwsCredentialsProvider>()
        val awsSchemaRegistryException =
            Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
                AWSSchemaRegistryClient(mockAwsCredentialsProvider, glueSchemaRegistryConfiguration)
            }
        assertEquals(URISyntaxException::class.java, awsSchemaRegistryException.cause!!.javaClass)

        val expectedMessage = "Malformed uri, please pass the valid uri for creating the client"
        assertEquals(expectedMessage, awsSchemaRegistryException.message)
    }

    /**
     * Tests positive case for querySchemaVersionMetadata by building request and response
     */
    @Test
    fun testQuerySchemaVersionMetadata_setSchemaVersionId_returnsResponseWithSchemaVersionId() {
        val querySchemaVersionMetadataResponse =
            QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()

        whenever(
            mockGlueClient!!.querySchemaVersionMetadata(
                QuerySchemaVersionMetadataRequest
                    .builder()
                    .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                    .build(),
            ),
        ).thenReturn(querySchemaVersionMetadataResponse)

        assertEquals(
            SCHEMA_ID_FOR_TESTING.toString(),
            awsSchemaRegistryClient.querySchemaVersionMetadata(SCHEMA_ID_FOR_TESTING).schemaVersionId(),
        )
    }

    /**
     * Tests negative case for querySchemaVersionMetadata by checking the AWSSchemaRegistryException exception
     */
    @Test
    fun testQuerySchemaVersionMetadata_clientThrowsException_throwsAWSSchemaRegistryException() {
        whenever(
            mockGlueClient!!.querySchemaVersionMetadata(
                QuerySchemaVersionMetadataRequest
                    .builder()
                    .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                    .build(),
            ),
        ).thenThrow(NullPointerException())

        try {
            awsSchemaRegistryClient.querySchemaVersionMetadata(SCHEMA_ID_FOR_TESTING)
        } catch (e: Exception) {
            assertEquals(AWSSchemaRegistryException::class.java, e.javaClass)
            val errorMessage =
                "Query schema version metadata :: Call failed when query metadata for schema version id = " +
                    SCHEMA_ID_FOR_TESTING.toString()
            assertEquals(errorMessage, e.message)
        }
    }

    /**
     * Tests buildGetSchemaByDefinitionRequest by verifying schema name and definition
     */
    @Test
    fun testBuildGetSchemaByDefinitionRequest_validConfigs_buildsResponseSuccessfully() {
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        val mockAwsCredentialsProvider = mock<AwsCredentialsProvider>()
        awsSchemaRegistryClient =
            AWSSchemaRegistryClient(mockAwsCredentialsProvider, glueSchemaRegistryConfiguration)

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(
                    userSchemaDefinition,
                    configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString(),
                )

        assertEquals(
            configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString(),
            getSchemaByDefinitionRequest.schemaId().schemaName(),
        )
        assertEquals(userSchemaDefinition, getSchemaByDefinitionRequest.schemaDefinition())
    }

    @Test
    fun testGetSchemaVersionIdByDefinition_nullSchemaVersionId_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(nullOf(), "test-schema-name", DataFormat.AVRO.name)
        }
    }

    @Test
    fun testGetSchemaVersionIdByDefinition_nullSchemaName_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(userSchemaDefinition, nullOf(), DataFormat.AVRO.name)
        }
    }

    @Test
    fun testGetSchemaVersionIdByDefinition_nullDataFormat_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(userSchemaDefinition, "test-schema-name", nullOf())
        }
    }

    @Test
    fun testGetSchemaVersionIdByDefinition_allParamsNonNull_schemaVersionIdMatches() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(
                    userSchemaDefinition,
                    configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString(),
                )
        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()

        whenever(mockGlueClient!!.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse)

        assertEquals(
            SCHEMA_ID_FOR_TESTING,
            awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                userSchemaDefinition,
                configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString(),
                DataFormat.AVRO.name,
            ),
        )
    }

    @Test
    fun testGetSchemaVersionIdByDefinition_clientExceptionResponse_throwsAWSSchemaRegistryException() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )
        mockGlueClient = null

        val awsSchemaRegistryException =
            assertThrows(AWSSchemaRegistryException::class.java) {
                awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
                    userSchemaDefinition,
                    configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString(),
                    DataFormat.AVRO.name,
                )
            }

        val expectedExceptionMessage =
            "Failed to get schemaVersionId by schema definition for schema name = " +
                configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString() + " "
        assertEquals(expectedExceptionMessage, awsSchemaRegistryException.message)
    }

    @Test
    fun testGetSchemaVersionResponse_nullSchemaVersionId_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            awsSchemaRegistryClient.getSchemaVersionResponse(nullOf())
        }
    }

    @Test
    fun testGetSchemaVersionResponse_setSchemaVersionId_returnsResponseSchemaVersionId() {
        val getSchemaVersionResponse =
            GetSchemaVersionResponse.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build()
        val getSchemaVersionRequest =
            GetSchemaVersionRequest.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build()
        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse)

        assertEquals(
            SCHEMA_ID_FOR_TESTING.toString(),
            awsSchemaRegistryClient.getSchemaVersionResponse(SCHEMA_ID_FOR_TESTING.toString()).schemaVersionId(),
        )
    }

    private fun getConfigsWithAutoRegistrationSetting(autoRegistrationSetting: Boolean): Map<String, String> {
        val localConfigs = HashMap<String, String>()
        localConfigs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        localConfigs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        localConfigs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        localConfigs[AWSSchemaRegistryConstants.REGISTRY_NAME] = "User-Topic"
        localConfigs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] =
            autoRegistrationSetting.toString()
        return localConfigs
    }

    @Test
    fun testGetSchemaVersionResponse_clientExceptionResponse_returnsAWSSchemaRegistryException() {
        val getSchemaVersionRequest =
            GetSchemaVersionRequest.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build()
        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest))
            .thenThrow(EntityNotFoundException::class.java)

        try {
            awsSchemaRegistryClient.getSchemaVersionResponse(SCHEMA_ID_FOR_TESTING.toString())
        } catch (e: Exception) {
            assertEquals(EntityNotFoundException::class.java, e.cause!!.javaClass)
            assertEquals(AWSSchemaRegistryException::class.java, e.javaClass)
            val expectedErrorMessage = "Failed to get schema version Id = $SCHEMA_ID_FOR_TESTING"
            assertEquals(expectedErrorMessage, e.message)
        }
    }

    @Test
    fun testCreateSchema_schemaNameWithDataFormat_returnsResponseSuccessfully() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
        val dataFormatName = DataFormat.AVRO.name
        val schemaVersionId = UUID.randomUUID().toString()

        val createSchemaResponse =
            CreateSchemaResponse
                .builder()
                .schemaName(schemaName)
                .dataFormat(dataFormatName)
                .schemaVersionId(schemaVersionId)
                .build()
        val createSchemaRequest =
            CreateSchemaRequest
                .builder()
                .dataFormat(DataFormat.AVRO)
                .description(glueSchemaRegistryConfiguration.description)
                .schemaName(schemaName)
                .schemaDefinition(userSchemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.compatibilitySetting)
                .tags(glueSchemaRegistryConfiguration.tags)
                .registryId(
                    RegistryId.builder().registryName(glueSchemaRegistryConfiguration.registryName).build(),
                ).build()

        whenever(mockGlueClient!!.createSchema(createSchemaRequest)).thenReturn(createSchemaResponse)
        assertEquals(
            UUID.fromString(schemaVersionId),
            awsSchemaRegistryClient.createSchema(schemaName, dataFormatName, userSchemaDefinition, getMetadata()),
        )
    }

    @Test
    fun testCreateSchema_clientExceptionResponse_returnsAWSSchemaRegistryException() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
        val dataFormatName = DataFormat.AVRO.name
        val createSchemaRequest =
            CreateSchemaRequest
                .builder()
                .dataFormat(DataFormat.AVRO)
                .description(glueSchemaRegistryConfiguration.description)
                .schemaName(schemaName)
                .schemaDefinition(userSchemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.compatibilitySetting)
                .tags(glueSchemaRegistryConfiguration.tags)
                .registryId(
                    RegistryId.builder().registryName(glueSchemaRegistryConfiguration.registryName).build(),
                ).build()

        whenever(mockGlueClient!!.createSchema(createSchemaRequest))
            .thenThrow(
                EntityNotFoundException
                    .builder()
                    .message("Schema registry entity not found")
                    .build(),
            )

        try {
            awsSchemaRegistryClient.createSchema(schemaName, dataFormatName, userSchemaDefinition, getMetadata())
        } catch (e: Exception) {
            assertEquals(EntityNotFoundException::class.java, e.cause!!.javaClass)
            assertEquals(AWSSchemaRegistryException::class.java, e.javaClass)
            val expectedErrorMessage =
                "Create schema :: Call failed when creating the schema with the schema registry for schema name = " +
                    schemaName + ". Error = Schema registry entity not found"
            assertEquals(expectedErrorMessage, e.message)
        }
    }

    @Test
    fun testRegisterSchemaVersion_validParameters_returnsResponseWithSchemaVersionId() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME].toString()
        val dataFormatName = DataFormat.AVRO.name
        val schemaVersionNumber = 1L
        val requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build()

        val registerSchemaVersionRequest =
            RegisterSchemaVersionRequest
                .builder()
                .schemaDefinition(userSchemaDefinition)
                .schemaId(requestSchemaId)
                .build()
        val registerSchemaVersionResponse =
            RegisterSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .versionNumber(schemaVersionNumber)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()
        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()

        whenever(mockGlueClient!!.registerSchemaVersion(registerSchemaVersionRequest))
            .thenReturn(registerSchemaVersionResponse)

        assertEquals(
            SCHEMA_ID_FOR_TESTING.toString(),
            awsSchemaRegistryClient
                .registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName)
                .schemaVersionId(),
        )
        verify(mockGlueClient!!, times(0)).getSchemaVersion(getSchemaVersionRequest)
    }

    @ParameterizedTest
    @EnumSource(
        value = AWSSchemaRegistryConstants.SchemaVersionStatus::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["AVAILABLE"],
    )
    fun testRegisterSchemaVersion_statusIsNotAvailable_throwsException(
        schemaVersionStatus: AWSSchemaRegistryConstants.SchemaVersionStatus,
    ) {
        val configs = getConfigsWithAutoRegistrationSetting(false)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val registerSchemaVersionResponse =
            RegisterSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(schemaVersionStatus.toString())
                .build()

        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()
        val getSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(schemaVersionStatus.toString())
                .build()

        whenever(mockGlueClient!!.registerSchemaVersion(any<RegisterSchemaVersionRequest>()))
            .thenReturn(registerSchemaVersionResponse)
        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest))
            .thenReturn(getSchemaVersionResponse)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName)
            }
        assertEquals(
            exception.message,
            "Register schema :: Call failed when registering the schema with the schema registry for " +
                "schema name = " + schemaName,
        )
        assertEquals(
            exception.cause!!.message,
            "Exception occurred, while performing schema evolution check for schemaVersionId = " +
                getSchemaVersionRequest.schemaVersionId(),
        )

        if (AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING == schemaVersionStatus) {
            verify(mockGlueClient!!, times(10)).getSchemaVersion(getSchemaVersionRequest)
        }

        if (AWSSchemaRegistryConstants.SchemaVersionStatus.DELETING == schemaVersionStatus ||
            AWSSchemaRegistryConstants.SchemaVersionStatus.FAILURE == schemaVersionStatus
        ) {
            verify(mockGlueClient!!, times(1)).getSchemaVersion(getSchemaVersionRequest)
        }
    }

    @Test
    fun testRegisterSchemaVersion_statusEvolvesToAvailable_succeeds() {
        val configs = getConfigsWithAutoRegistrationSetting(false)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME].toString()
        val dataFormatName = DataFormat.AVRO.name

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build()

        val registerSchemaVersionRequest =
            RegisterSchemaVersionRequest
                .builder()
                .schemaDefinition(userSchemaDefinition)
                .schemaId(requestSchemaId)
                .build()

        val registerSchemaVersionResponse =
            RegisterSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString())
                .build()

        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()

        val getSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()

        whenever(mockGlueClient!!.registerSchemaVersion(registerSchemaVersionRequest))
            .thenReturn(registerSchemaVersionResponse)
        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse)

        assertEquals(
            SCHEMA_ID_FOR_TESTING.toString(),
            awsSchemaRegistryClient
                .registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName)
                .schemaVersionId(),
        )
        verify(mockGlueClient!!, times(1)).getSchemaVersion(getSchemaVersionRequest)
    }

    @Test
    fun testRegisterSchemaVersion_clientThrowsException_throwsAWSSchemaRegistryException() {
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
        val dataFormatName = DataFormat.AVRO.name
        mockGlueClient = null
        assertThrows(AWSSchemaRegistryException::class.java) {
            awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName)
        }
    }

    @Test
    fun testGetSchemaIdRequestObject_nullSchemaName_throwsException() {
        val getSchemaIdRequestObjectMethod =
            AWSSchemaRegistryClient::class.java
                .getDeclaredMethod("getSchemaIdRequestObject", String::class.java, String::class.java)
        getSchemaIdRequestObjectMethod.isAccessible = true

        try {
            getSchemaIdRequestObjectMethod.invoke(awsSchemaRegistryClient, null, "test-registry-name")
        } catch (e: Exception) {
            assertEquals(IllegalArgumentException::class.java, e.cause!!.javaClass)
        }
    }

    @Test
    fun testGetSchemaIdRequestObject_nullRegistryName_throwsException() {
        val getSchemaIdRequestObjectMethod =
            AWSSchemaRegistryClient::class.java
                .getDeclaredMethod("getSchemaIdRequestObject", String::class.java, String::class.java)
        getSchemaIdRequestObjectMethod.isAccessible = true

        try {
            getSchemaIdRequestObjectMethod.invoke(awsSchemaRegistryClient, "test-schema-name", null)
        } catch (e: Exception) {
            assertEquals(IllegalArgumentException::class.java, e.cause!!.javaClass)
        }
    }

    @Test
    fun testValidateSchemaVersionResponse_nullSchemaName_throwsException() {
        val validateSchemaVersionResponseMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "validateSchemaVersionResponse",
                GetSchemaVersionResponse::class.java,
                String::class.java,
            )
        validateSchemaVersionResponseMethod.isAccessible = true
        val getSchemaVersionResponse = GetSchemaVersionResponse.builder().build()

        try {
            validateSchemaVersionResponseMethod.invoke(awsSchemaRegistryClient, getSchemaVersionResponse, null)
        } catch (e: Exception) {
            val exceptionMessage = "Schema definition is not present for the schema id = null"
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
            assertEquals(exceptionMessage, e.cause!!.message)
        }
    }

    @Test
    fun testValidateSchemaVersionResponse_nullGetSchemaVersionResponse_throwsException() {
        val validateSchemaVersionResponseMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "validateSchemaVersionResponse",
                GetSchemaVersionResponse::class.java,
                String::class.java,
            )
        validateSchemaVersionResponseMethod.isAccessible = true

        try {
            validateSchemaVersionResponseMethod.invoke(awsSchemaRegistryClient, null, "test-schema-name")
        } catch (e: Exception) {
            val exceptionMessage = "Schema definition is not present for the schema id = test-schema-name"
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
            assertEquals(exceptionMessage, e.cause!!.message)
        }
    }

    @Test
    fun testReturnSchemaVersionIdIfAvailable_nullSchemaVersionId_throwsException() {
        val returnSchemaVersionIdIfAvailableMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "returnSchemaVersionIdIfAvailable",
                GetSchemaByDefinitionResponse::class.java,
            )
        returnSchemaVersionIdIfAvailableMethod.isAccessible = true
        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(null)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()

        try {
            returnSchemaVersionIdIfAvailableMethod.invoke(awsSchemaRegistryClient, getSchemaByDefinitionResponse)
        } catch (e: Exception) {
            val exceptionMessage = "Schema Found but status is " + getSchemaByDefinitionResponse.statusAsString()
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
            assertEquals(exceptionMessage, e.cause!!.message)
        }
    }

    @Test
    fun testReturnSchemaVersionIdIfAvailable_nullStatusString_throwsException() {
        val returnSchemaVersionIdIfAvailableMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "returnSchemaVersionIdIfAvailable",
                GetSchemaByDefinitionResponse::class.java,
            )
        returnSchemaVersionIdIfAvailableMethod.isAccessible = true
        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status("invalidStatus")
                .build()

        try {
            returnSchemaVersionIdIfAvailableMethod.invoke(awsSchemaRegistryClient, getSchemaByDefinitionResponse)
        } catch (e: Exception) {
            val exceptionMessage = "Schema Found but status is " + getSchemaByDefinitionResponse.statusAsString()
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
            assertEquals(exceptionMessage, e.cause!!.message)
        }
    }

    @Test
    fun testWaitForSchemaEvolutionCheckToComplete_resultsAvailableResponse_returnsResponseWithSchemaId() {
        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()
        val getSchemaVersionResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()

        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse)
        val waitForSchemaEvolutionCheckToCompleteMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "waitForSchemaEvolutionCheckToComplete",
                GetSchemaVersionRequest::class.java,
            )
        waitForSchemaEvolutionCheckToCompleteMethod.isAccessible = true
        val resultResponse =
            assertDoesNotThrow<Any> {
                waitForSchemaEvolutionCheckToCompleteMethod.invoke(awsSchemaRegistryClient, getSchemaVersionRequest)
            } as GetSchemaVersionResponse

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), resultResponse.schemaVersionId())
    }

    @Test
    fun testWaitForSchemaEvolutionCheckToComplete_pendingThenAvailable_waitsBetweenAttempts() {
        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()
        val pendingResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString())
                .build()
        val availableResponse =
            GetSchemaVersionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build()

        whenever(mockGlueClient!!.getSchemaVersion(getSchemaVersionRequest))
            .thenReturn(pendingResponse, availableResponse)
        val waitForSchemaEvolutionCheckToCompleteMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "waitForSchemaEvolutionCheckToComplete",
                GetSchemaVersionRequest::class.java,
            )
        waitForSchemaEvolutionCheckToCompleteMethod.isAccessible = true

        val startedAt = System.nanoTime()
        val resultResponse =
            assertDoesNotThrow<Any> {
                waitForSchemaEvolutionCheckToCompleteMethod.invoke(awsSchemaRegistryClient, getSchemaVersionRequest)
            } as GetSchemaVersionResponse
        val elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), resultResponse.schemaVersionId())
        verify(mockGlueClient!!, times(2)).getSchemaVersion(getSchemaVersionRequest)
        assertTrue(
            elapsedMillis >= 3100,
            "Expected the second attempt to be delayed, took $elapsedMillis ms",
        )
    }

    @Test
    fun testWaitForSchemaEvolutionCheckToComplete_clientThrowsException_throwsException() {
        val getSchemaVersionRequest =
            GetSchemaVersionRequest
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build()

        mockGlueClient = null
        val waitForSchemaEvolutionCheckToCompleteMethod =
            AWSSchemaRegistryClient::class.java.getDeclaredMethod(
                "waitForSchemaEvolutionCheckToComplete",
                GetSchemaVersionRequest::class.java,
            )
        waitForSchemaEvolutionCheckToCompleteMethod.isAccessible = true

        try {
            waitForSchemaEvolutionCheckToCompleteMethod.invoke(awsSchemaRegistryClient, getSchemaVersionRequest)
        } catch (e: Exception) {
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
            val expectedExceptionMessage =
                "Exception occurred, while performing schema evolution check for schemaVersionId = " +
                    getSchemaVersionRequest.schemaVersionId()
            assertEquals(expectedExceptionMessage, e.cause!!.message)
        }
    }

    @Test
    fun testQuerySchemaTags_validGetTagsRequest_returnsValidResponse() {
        val testSchemaName = "test-schema"
        val testSchemaDefinition = "test-schema-definition"
        val testSchemaARN = "test-schema-arn"
        val expectedTags = glueSchemaRegistryConfiguration.tags
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse.builder().schemaArn(testSchemaARN).build()
        println("getSchemaByDefinitionResponse:$getSchemaByDefinitionResponse")

        whenever(
            mockGlueClient!!.getSchemaByDefinition(
                awsSchemaRegistryClient.buildGetSchemaByDefinitionRequest(testSchemaDefinition, testSchemaName),
            ),
        ).thenReturn(getSchemaByDefinitionResponse)

        val getTagsRequest = GetTagsRequest.builder().resourceArn(testSchemaARN).build()
        val getTagsResponse = GetTagsResponse.builder().tags(expectedTags).build()
        whenever(mockGlueClient!!.getTags(getTagsRequest)).thenReturn(getTagsResponse)

        val responseTags: Map<String, String> =
            assertDoesNotThrow<Map<String, String>> {
                awsSchemaRegistryClient.querySchemaTags(testSchemaDefinition, testSchemaName).tags()
            }

        assertNotNull(responseTags)
        assertEquals(expectedTags.size, responseTags.size)
        assertTrue(expectedTags.containsKey("testKey"))
        assertEquals(expectedTags["testKey"], responseTags["testKey"])
    }

    @Test
    fun testQuerySchemaTags_clientThrowsException_throwsException() {
        val testSchemaName = "test-schema"
        val testSchemaDefinition = "test-schema-definition"
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )
        mockGlueClient = null

        val awsSchemaRegistryException =
            assertThrows(AWSSchemaRegistryException::class.java) {
                awsSchemaRegistryClient.querySchemaTags(testSchemaDefinition, testSchemaName)
            }

        val expectedMsg = "Query schema tags:: Call failed while querying tags for schema = $testSchemaName"
        assertEquals(expectedMsg, awsSchemaRegistryException.message)
    }

    private fun getMetadata(): Map<String, String> {
        val metadata = HashMap<String, String>()
        metadata["event-source-1"] = "topic1"
        metadata["event-source-2"] = "topic2"
        metadata["event-source-3"] = "topic3"
        metadata["event-source-4"] = "topic4"
        metadata["event-source-5"] = "topic5"

        return metadata
    }

    private fun createMetadataKeyValuePair(metadataEntry: Map.Entry<String, String>): MetadataKeyValuePair = MetadataKeyValuePair
        .builder()
        .metadataKey(metadataEntry.key)
        .metadataValue(metadataEntry.value)
        .build()

    private fun createPutSchemaVersionMetadataRequest(
        schemaVersionId: UUID,
        metadataKeyValuePair: MetadataKeyValuePair,
    ): PutSchemaVersionMetadataRequest = PutSchemaVersionMetadataRequest
        .builder()
        .schemaVersionId(schemaVersionId.toString())
        .metadataKeyValue(metadataKeyValuePair)
        .build()

    private fun createPutSchemaVersionMetadataResponse(
        schemaVersionId: UUID,
        metadataKeyValuePair: MetadataKeyValuePair,
    ): PutSchemaVersionMetadataResponse = PutSchemaVersionMetadataResponse
        .builder()
        .schemaVersionId(schemaVersionId.toString())
        .metadataKey(metadataKeyValuePair.metadataKey())
        .metadataValue(metadataKeyValuePair.metadataValue())
        .build()

    private fun configureAWSSchemaRegistryClientWithSerdeConfig(
        awsSchemaRegistryClient: AWSSchemaRegistryClient,
        glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
    ): AWSSchemaRegistryClient {
        val serdeConfigField =
            AWSSchemaRegistryClient::class.java.getDeclaredField("glueSchemaRegistryConfiguration")
        serdeConfigField.isAccessible = true
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)

        return awsSchemaRegistryClient
    }

    companion object {
        private val SCHEMA_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
        const val AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc"
    }
}
