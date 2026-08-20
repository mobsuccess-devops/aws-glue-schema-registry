package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.RegistryId
import software.amazon.awssdk.services.glue.model.SchemaId
import java.util.UUID

class SchemaByDefinitionFetcherTest {
    private lateinit var awsSchemaRegistryClient: AWSSchemaRegistryClient
    private lateinit var schemaByDefinitionFetcher: SchemaByDefinitionFetcher

    private lateinit var mockGlueClient: GlueClient
    private lateinit var userSchemaDefinition: String

    @BeforeEach
    fun setUp() {
        mockGlueClient = mock<GlueClient>()
        awsSchemaRegistryClient = AWSSchemaRegistryClient(mockGlueClient)
        val config = GlueSchemaRegistryConfiguration(getConfigsWithAutoRegistrationSetting(true))
        schemaByDefinitionFetcher = SchemaByDefinitionFetcher(awsSchemaRegistryClient, config)
        userSchemaDefinition = "{Some-avro-schema}"
    }

    @Test
    fun testGetORRegisterSchemaVersionId_schemaVersionNotPresent_autoRegistersSchemaVersion() {
        val configs = getConfigsWithAutoRegistrationSetting(true)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val entityNotFoundException =
            EntityNotFoundException
                .builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)
                .build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenThrow(awsSchemaRegistryException)

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
                .build()
        whenever(mockGlueClient.registerSchemaVersion(registerSchemaVersionRequest))
            .thenReturn(registerSchemaVersionResponse)

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
        whenever(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse)

        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)

        val schemaVersionId =
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId)
    }

    @Test
    fun testGetORRegisterSchemaVersionId_nullSchemaDefinition_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(nullOf(), "test-schema-name", DataFormat.AVRO.name, getMetadata())
        }
    }

    @Test
    fun testGetORRegisterSchemaVersionId_nullSchemaSchemaName_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, nullOf(), DataFormat.AVRO.name, getMetadata())
        }
    }

    @Test
    fun testGetORRegisterSchemaVersionId_nullSchemaDataFormat_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, "", nullOf(), getMetadata())
        }
    }

    @Test
    fun testGetORRegisterSchemaVersionId_nullMetadata_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, "", DataFormat.AVRO.toString(), nullOf())
        }
    }

    @Test
    fun testGetORRegisterSchemaVersionId_WhenVersionIsPresent_ReturnsIt() {
        val configs = getConfigsWithAutoRegistrationSetting(true)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val awsSchemaRegistrySerDeConfigs = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs)

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build()

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse)

        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs)

        val schemaVersionId =
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId)
    }

    @Test
    fun testGetORRegisterSchemaVersionId_OnUnknownException_ThrowsException() {
        val configs = getConfigsWithAutoRegistrationSetting(true)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val awsSchemaRegistrySerDeConfigs = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs)

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val awsSchemaRegistryException = AWSSchemaRegistryException(RuntimeException("Unknown"))

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenThrow(awsSchemaRegistryException)

        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                schemaByDefinitionFetcher
                    .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
            }
        assertTrue(
            exception.message!!.contains("Exception occurred while fetching or registering schema definition"),
        )
        assertTrue(
            exception.message!!.contains("Error: java.lang.RuntimeException: Unknown"),
        )
    }

    @Test
    fun testGetORRegisterSchemaVersionId_schemaNotPresent_autoCreatesSchema() {
        val configs = getConfigsWithAutoRegistrationSetting(true)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val entityNotFoundException =
            EntityNotFoundException
                .builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                .build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenThrow(awsSchemaRegistryException)

        val createSchemaResponse =
            CreateSchemaResponse
                .builder()
                .schemaName(schemaName)
                .dataFormat(dataFormatName)
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
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

        whenever(mockGlueClient.createSchema(createSchemaRequest)).thenReturn(createSchemaResponse)

        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)

        val schemaVersionId =
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId)
    }

    @Test
    fun testGetORRegisterSchemaVersionId_autoRegistrationDisabled_failsIfSchemaVersionNotPresent() {
        val configs = getConfigsWithAutoRegistrationSetting(false)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val entityNotFoundException =
            EntityNotFoundException
                .builder()
                .message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                .build()
        val awsSchemaRegistryException = AWSSchemaRegistryException(entityNotFoundException)

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenThrow(awsSchemaRegistryException)

        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                schemaByDefinitionFetcher
                    .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
            }

        assertEquals(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG, exception.message)
    }

    @Test
    fun testGetORRegisterSchemaVersionId_retrieveSchemaVersionId_schemaVersionIdIsCached() {
        val configs = getConfigsWithAutoRegistrationSetting(false)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build()

        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse)

        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )
        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)
        val cache = schemaByDefinitionFetcher.schemaDefinitionToVersionCache

        // Ensure cache is empty to start with.
        assertEquals(0, cache.size())

        // First call
        schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        // Second call
        schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        // Third call
        schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())

        // Ensure cache is populated
        assertEquals(1, cache.size())

        val expectedSchema = Schema(userSchemaDefinition, dataFormatName, schemaName)

        @Suppress("UNCHECKED_CAST")
        val cacheEntry =
            cache
                .asMap()
                .entries
                .toTypedArray()[0] as Map.Entry<Schema, UUID>

        // Ensure cache entries are expected
        assertEquals(expectedSchema, cacheEntry.key)
        assertEquals(SCHEMA_ID_FOR_TESTING, cacheEntry.value)

        // Ensure only 1 call happened.
        verify(mockGlueClient, times(1)).getSchemaByDefinition(getSchemaByDefinitionRequest)
    }

    @Test
    fun testGetORRegisterSchemaVersionId_continuesToServeFromCache_WhenCallsFail() {
        val configs = getConfigsWithAutoRegistrationSetting(false)

        val schemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME]!!
        val registryName = configs[AWSSchemaRegistryConstants.REGISTRY_NAME]!!
        val dataFormatName = DataFormat.AVRO.name
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)

        // Override TTL to 1s.
        glueSchemaRegistryConfiguration.timeToLiveMillis = 1000L

        val getSchemaByDefinitionRequest =
            awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName)

        val getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build()

        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(
                awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration,
            )
        schemaByDefinitionFetcher =
            SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration)
        val cache = schemaByDefinitionFetcher.schemaDefinitionToVersionCache

        // Ensure cache is empty to start with.
        assertEquals(0, cache.size())

        // Mock the client to return response, then fail and eventually succeed.
        whenever(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse)
            .thenThrow(RuntimeException("Service outage"))
            .thenThrow(RuntimeException("Service outage"))
            .thenReturn(getSchemaByDefinitionResponse)

        // First call
        // As expected first call should fetch and cache the schema version.
        assertDoesNotThrow {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        }
        assertEquals(1, cache.size())

        // Wait for 1.5 seconds to expire cache.
        Thread.sleep(1500L)

        // Second call shouldn't fail.
        assertDoesNotThrow {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        }

        // Third call shouldn't fail.
        assertDoesNotThrow {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        }

        // Verify the entry is not evicted.
        assertEquals(1, cache.size())

        // Fourth call shouldn't fail and cache is refreshed.
        assertDoesNotThrow {
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata())
        }
        verify(mockGlueClient, times(4)).getSchemaByDefinition(getSchemaByDefinitionRequest)
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

    private fun getMetadata(): Map<String, String> {
        val metadata = HashMap<String, String>()
        metadata["event-source-1"] = "topic1"
        metadata["event-source-2"] = "topic2"
        metadata["event-source-3"] = "topic3"
        metadata["event-source-4"] = "topic4"
        metadata["event-source-5"] = "topic5"
        return metadata
    }

    companion object {
        private val SCHEMA_ID_FOR_TESTING = UUID.fromString("f8b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
    }
}
