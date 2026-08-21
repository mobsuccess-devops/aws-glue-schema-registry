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

import com.amazonaws.services.schemaregistry.common.compatibility.JsonSchemaCompatibilityChecker
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ImmutableMap
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.ApiName
import software.amazon.awssdk.core.SdkRequest
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.glue.GlueClient
import software.amazon.awssdk.services.glue.model.AlreadyExistsException
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest
import software.amazon.awssdk.services.glue.model.DataFormat
import software.amazon.awssdk.services.glue.model.EntityNotFoundException
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.GetTagsRequest
import software.amazon.awssdk.services.glue.model.GetTagsResponse
import software.amazon.awssdk.services.glue.model.GlueRequest
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse
import software.amazon.awssdk.services.glue.model.RegistryId
import software.amazon.awssdk.services.glue.model.SchemaId
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber
import java.net.URI
import java.net.URISyntaxException
import java.util.StringJoiner
import java.util.UUID

/**
 * Handles all the requests related to the schema management.
 *
 * `open`: several test suites mock this type, and Kotlin classes and methods are final
 * by default.
 */
open class AWSSchemaRegistryClient {
    private val client: GlueClient
    private var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration? = null

    /**
     * Create Amazon Schema Registry Client.
     *
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    constructor(
        credentialsProvider: AwsCredentialsProvider,
        glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
        retryPolicy: RetryPolicy,
    ) {
        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration
        val overrideConfiguration =
            ClientOverrideConfiguration
                .builder()
                .retryPolicy(retryPolicy)
                .addExecutionInterceptor(UserAgentRequestInterceptor())
                .build()

        val urlConnectionHttpClientBuilder = UrlConnectionHttpClient.builder()
        val proxyUrl = glueSchemaRegistryConfiguration.proxyUrl
        if (proxyUrl != null) {
            log.debug("Creating http client using proxy {}", proxyUrl.toString())
            urlConnectionHttpClientBuilder.proxyConfiguration(
                ProxyConfiguration.builder().endpoint(proxyUrl).build(),
            )
        }

        val glueClientBuilder =
            GlueClient
                .builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfiguration)
                .httpClient(urlConnectionHttpClientBuilder.build())
                .region(Region.of(glueSchemaRegistryConfiguration.region))

        val endPoint = glueSchemaRegistryConfiguration.endPoint
        if (endPoint != null) {
            try {
                glueClientBuilder.endpointOverride(URI(endPoint))
            } catch (e: URISyntaxException) {
                throw AWSSchemaRegistryException("Malformed uri, please pass the valid uri for creating the client", e)
            }
        }
        client = glueClientBuilder.build()
    }

    /**
     * Create Amazon Schema Registry Client.
     *
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    constructor(
        credentialsProvider: AwsCredentialsProvider,
        glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
    ) : this(credentialsProvider, glueSchemaRegistryConfiguration, RetryPolicy.defaultRetryPolicy())

    constructor(glueClient: GlueClient) {
        client = glueClient
    }

    private val configuration: GlueSchemaRegistryConfiguration
        get() = glueSchemaRegistryConfiguration!!

    private val jsonSchemaCompatibilityChecker = JsonSchemaCompatibilityChecker()

    /**
     * Get Schema Version ID by passing the schema definition.
     *
     * @throws AWSSchemaRegistryException on any error while fetching the schema version ID
     */
    open fun getSchemaVersionIdByDefinition(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
    ): UUID {
        try {
            log.debug(
                "Getting Schema Version Id for : schemaDefinition = {}, schemaName = {}, dataFormat = {}",
                schemaDefinition,
                schemaName,
                dataFormat,
            )
            val response = client.getSchemaByDefinition(buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName))
            return returnSchemaVersionIdIfAvailable(response)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Failed to get schemaVersionId by schema definition for schema name = $schemaName ",
                e,
            )
        }
    }

    /**
     * Get the schema definition by passing the schema id.
     *
     * @throws AWSSchemaRegistryException on any errors during schema retrieval from service
     */
    open fun getSchemaVersionResponse(schemaVersionId: String): GetSchemaVersionResponse {
        try {
            val schemaVersionResponse = client.getSchemaVersion(getSchemaVersionRequest(schemaVersionId))
            validateSchemaVersionResponse(schemaVersionResponse, schemaVersionId)
            return schemaVersionResponse
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("Failed to get schema version Id = $schemaVersionId", e)
        }
    }

    private fun getSchemaVersionRequest(schemaVersionId: String): GetSchemaVersionRequest = GetSchemaVersionRequest.builder().schemaVersionId(schemaVersionId).build()

    private fun validateSchemaVersionResponse(
        schemaVersionResponse: GetSchemaVersionResponse?,
        schemaVersionId: String,
    ) {
        if (schemaVersionResponse?.schemaVersionId() == null) {
            throw AWSSchemaRegistryException("Schema definition is not present for the schema id = $schemaVersionId")
        }
    }

    private fun returnSchemaVersionIdIfAvailable(response: GetSchemaByDefinitionResponse): UUID {
        if (response.schemaVersionId() != null &&
            response.statusAsString() == AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
        ) {
            return UUID.fromString(response.schemaVersionId())
        }
        throw AWSSchemaRegistryException("Schema Found but status is ${response.statusAsString()}")
    }

    /**
     * Create a request to get a schema using the schema definition and the schema name.
     */
    open fun buildGetSchemaByDefinitionRequest(
        schemaDefinition: String,
        schemaName: String,
    ): GetSchemaByDefinitionRequest = buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName, configuration.registryName)

    /**
     * Create a request to get a schema using the schema definition, the schema name and the registry.
     */
    open fun buildGetSchemaByDefinitionRequest(
        schemaDefinition: String,
        schemaName: String,
        registryName: String?,
    ): GetSchemaByDefinitionRequest = GetSchemaByDefinitionRequest
        .builder()
        .schemaId(getSchemaIdRequestObject(schemaName, registryName))
        .schemaDefinition(schemaDefinition)
        .build()

    /**
     * Create a schema using the Glue client and return the schema version id.
     *
     * @throws AWSSchemaRegistryException on any error during the schema creation
     */
    open fun createSchema(
        schemaName: String,
        dataFormat: String,
        schemaDefinition: String,
        metadata: Map<String, String>,
    ): UUID {
        val schemaVersionId: UUID =
            try {
                log.info(
                    "Auto Creating schema with schemaName: {} and schemaDefinition : {}",
                    schemaName,
                    schemaDefinition,
                )
                val createSchemaResponse =
                    client.createSchema(getCreateSchemaRequestObject(schemaName, dataFormat, schemaDefinition))
                UUID.fromString(createSchemaResponse.schemaVersionId())
            } catch (e: AlreadyExistsException) {
                log.warn(
                    "Schema is already created, this could be caused by multiple producers racing to " +
                        "auto-create schema.",
                )
                registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata)
            } catch (e: Exception) {
                throw AWSSchemaRegistryException(
                    "Create schema :: Call failed when creating the schema with the schema registry for " +
                        "schema name = $schemaName. Error = ${e.message}",
                    e,
                )
            }

        putSchemaVersionMetadata(schemaVersionId, metadata)
        return schemaVersionId
    }

    /**
     * Register the schema and return schema version Id once it is available.
     *
     * @throws AWSSchemaRegistryException on any error during the registration
     */
    open fun registerSchemaVersion(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
        metadata: Map<String, String>,
    ): UUID {
        val getSchemaVersionResponse = registerSchemaVersion(schemaDefinition, schemaName, dataFormat)
        val schemaVersionId = UUID.fromString(getSchemaVersionResponse.schemaVersionId())
        putSchemaVersionMetadata(schemaVersionId, metadata)
        return schemaVersionId
    }

    /**
     * Register the schema and return the get schema version response once it is available.
     *
     * @throws AWSSchemaRegistryException on any error during the registration
     */
    open fun registerSchemaVersion(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
    ): GetSchemaVersionResponse {
        checkJsonSchemaCompatibility(schemaDefinition, schemaName, dataFormat)

        try {
            val registerSchemaVersionResponse =
                client.registerSchemaVersion(getRegisterSchemaVersionRequest(schemaDefinition, schemaName))

            log.info(
                "Registered the schema version with schema version id = {} and with version number = {} and status {}",
                registerSchemaVersionResponse.schemaVersionId(),
                registerSchemaVersionResponse.versionNumber(),
                registerSchemaVersionResponse.statusAsString(),
            )

            if (AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString() ==
                registerSchemaVersionResponse.statusAsString()
            ) {
                return transformToGetSchemaVersionResponse(registerSchemaVersionResponse)
            }

            return waitForSchemaEvolutionCheckToComplete(
                getGetSchemaVersionRequest(registerSchemaVersionResponse.schemaVersionId()),
            )
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Register schema :: Call failed when registering the schema with the schema registry " +
                    "for schema name = $schemaName",
                e,
            )
        }
    }

    private fun transformToGetSchemaVersionResponse(
        registerSchemaVersionResponse: RegisterSchemaVersionResponse,
    ): GetSchemaVersionResponse = GetSchemaVersionResponse
        .builder()
        .schemaVersionId(registerSchemaVersionResponse.schemaVersionId())
        .status(registerSchemaVersionResponse.status())
        .status(registerSchemaVersionResponse.statusAsString())
        .versionNumber(registerSchemaVersionResponse.versionNumber())
        .build()

    private fun getCreateSchemaRequestObject(
        schemaName: String,
        dataFormat: String,
        schemaDefinition: String,
    ): CreateSchemaRequest = CreateSchemaRequest
        .builder()
        .dataFormat(DataFormat.valueOf(dataFormat))
        .description(configuration.description)
        .registryId(RegistryId.builder().registryName(configuration.registryName).build())
        .schemaName(schemaName)
        .schemaDefinition(schemaDefinition)
        .compatibility(configuration.compatibilitySetting)
        .tags(configuration.tags)
        .build()

    private fun getRegisterSchemaVersionRequest(
        schemaDefinition: String,
        schemaName: String,
    ): RegisterSchemaVersionRequest = RegisterSchemaVersionRequest
        .builder()
        .schemaDefinition(schemaDefinition)
        .schemaId(getSchemaIdRequestObject(schemaName, configuration.registryName))
        .build()

    private fun checkJsonSchemaCompatibility(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
    ) {
        val configuration = glueSchemaRegistryConfiguration ?: return
        if (!configuration.isJsonSchemaCompatibilityCheckEnabled ||
            !DataFormat.JSON.toString().equals(dataFormat, ignoreCase = true)
        ) {
            return
        }

        val previousSchemaDefinition =
            try {
                client
                    .getSchemaVersion(getLatestSchemaVersionRequest(schemaName, configuration.registryName))
                    .schemaDefinition()
            } catch (e: EntityNotFoundException) {
                log.debug("No previous version of schema {} to check compatibility against", schemaName, e)
                return
            } catch (e: Exception) {
                log.warn(
                    "Could not read the latest version of schema {}; skipping the client-side compatibility check",
                    schemaName,
                    e,
                )
                return
            }

        val errors =
            jsonSchemaCompatibilityChecker.checkCompatibility(
                schemaDefinition,
                previousSchemaDefinition,
                configuration.compatibilitySetting,
            )
        if (errors.isNotEmpty()) {
            throw AWSSchemaRegistryException(
                "Schema compatibility check failed for schema $schemaName under " +
                    "${configuration.compatibilitySetting} compatibility : ${errors.joinToString("; ")}",
            )
        }
    }

    private fun getLatestSchemaVersionRequest(
        schemaName: String,
        registryName: String?,
    ): GetSchemaVersionRequest = GetSchemaVersionRequest
        .builder()
        .schemaId(getSchemaIdRequestObject(schemaName, registryName))
        .schemaVersionNumber(SchemaVersionNumber.builder().latestVersion(true).build())
        .build()

    private fun getSchemaIdRequestObject(
        schemaName: String,
        registryName: String?,
    ): SchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build()

    private fun getGetSchemaVersionRequest(schemaVersionId: String): GetSchemaVersionRequest = GetSchemaVersionRequest.builder().schemaVersionId(schemaVersionId).build()

    /**
     * Get schema version response of asynchronous operation.
     */
    private fun waitForSchemaEvolutionCheckToComplete(
        getSchemaVersionRequest: GetSchemaVersionRequest,
    ): GetSchemaVersionResponse {
        val response: GetSchemaVersionResponse
        try {
            var retries = 0
            Thread.sleep(MAX_WAIT_INTERVAL)

            var current: GetSchemaVersionResponse
            do {
                if (retries > 0) {
                    Thread.sleep(retryWaitMillis(retries))
                }
                current = client.getSchemaVersion(getSchemaVersionRequest)

                if (AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString() == current.statusAsString()) {
                    return current
                } else if (AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString() !=
                    current.statusAsString()
                ) {
                    throw AWSSchemaRegistryException(
                        "Schema evolution check failed. schemaVersionId " +
                            "${getSchemaVersionRequest.schemaVersionId()} is in ${current.statusAsString()} status.",
                    )
                }
            } while (retries++ < MAX_ATTEMPTS - 1)

            if (retries >= MAX_ATTEMPTS &&
                AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString() != current.statusAsString()
            ) {
                throw AWSSchemaRegistryException(
                    "Retries exhausted for schema evolution check for schemaVersionId = " +
                        "${getSchemaVersionRequest.schemaVersionId()}",
                )
            }
            response = current
        } catch (ex: Exception) {
            throw AWSSchemaRegistryException(
                "Exception occurred, while performing schema evolution check for schemaVersionId = " +
                    "${getSchemaVersionRequest.schemaVersionId()}",
                ex,
            )
        }
        return response
    }

    private fun retryWaitMillis(attempt: Int): Long = minOf(BASE_RETRY_WAIT_INTERVAL shl (attempt - 1), MAX_WAIT_INTERVAL)

    /**
     * Put metadata to schema version asynchronously.
     */
    open fun putSchemaVersionMetadata(
        schemaVersionId: UUID,
        metadata: Map<String, String>,
    ) {
        metadata.entries
            .parallelStream()
            .map { createMetadataKeyValuePair(it) }
            .forEach { metadataKeyValuePair ->
                try {
                    putSchemaVersionMetadata(schemaVersionId, metadataKeyValuePair)
                } catch (e: AWSSchemaRegistryException) {
                    log.warn(e.message)
                }
            }
    }

    /**
     * Put metadata to schema version and return the response object.
     *
     * @throws AWSSchemaRegistryException on any error during putting metadata
     */
    open fun putSchemaVersionMetadata(
        schemaVersionId: UUID,
        metadataKeyValuePair: MetadataKeyValuePair,
    ): PutSchemaVersionMetadataResponse {
        try {
            return client.putSchemaVersionMetadata(
                createPutSchemaVersionMetadataRequest(schemaVersionId, metadataKeyValuePair),
            )
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Put schema version metadata :: Call failed when put metadata key = " +
                    "${metadataKeyValuePair.metadataKey()} value = ${metadataKeyValuePair.metadataValue()} " +
                    "to schema for schema version id = $schemaVersionId",
                e,
            )
        }
    }

    private fun createPutSchemaVersionMetadataRequest(
        schemaVersionId: UUID,
        metadataKeyValuePair: MetadataKeyValuePair,
    ): PutSchemaVersionMetadataRequest = PutSchemaVersionMetadataRequest
        .builder()
        .schemaVersionId(schemaVersionId.toString())
        .metadataKeyValue(metadataKeyValuePair)
        .build()

    private fun createMetadataKeyValuePair(metadataEntry: Map.Entry<String, String>): MetadataKeyValuePair = MetadataKeyValuePair
        .builder()
        .metadataKey(metadataEntry.key)
        .metadataValue(metadataEntry.value)
        .build()

    /**
     * Query metadata for schema version and return the response object.
     *
     * @throws AWSSchemaRegistryException on any error during querying metadata
     */
    open fun querySchemaVersionMetadata(schemaVersionId: UUID): QuerySchemaVersionMetadataResponse {
        try {
            return client.querySchemaVersionMetadata(createQuerySchemaVersionMetadataRequest(schemaVersionId))
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Query schema version metadata :: Call failed when query metadata for schema version id = " +
                    "$schemaVersionId",
                e,
            )
        }
    }

    private fun createQuerySchemaVersionMetadataRequest(schemaVersionId: UUID): QuerySchemaVersionMetadataRequest = QuerySchemaVersionMetadataRequest.builder().schemaVersionId(schemaVersionId.toString()).build()

    /**
     * Query Schema Tags Response for a given schema name and definition.
     */
    open fun querySchemaTags(
        schemaDefinition: String,
        schemaName: String,
    ): GetTagsResponse {
        try {
            val getSchemaByDefinitionResponse =
                client.getSchemaByDefinition(buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName))
            val getTagsRequest =
                GetTagsRequest.builder().resourceArn(getSchemaByDefinitionResponse.schemaArn()).build()
            return client.getTags(getTagsRequest)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Query schema tags:: Call failed while querying tags for schema = $schemaName",
                e,
            )
        }
    }

    /**
     * AWS SDK Request interceptor that adds additional data to the UserAgent of Glue API requests.
     */
    @VisibleForTesting
    internal open inner class UserAgentRequestInterceptor : ExecutionInterceptor {
        // executionAttributes stays nullable: the Java signature did not annotate it and
        // the tests call the method with null.
        override fun modifyRequest(
            context: Context.ModifyRequest,
            executionAttributes: ExecutionAttributes?,
        ): SdkRequest {
            val request = context.request()
            if (request !is GlueRequest) {
                // Only applies to Glue requests.
                return request
            }

            val overrideConfiguration =
                request
                    .overrideConfiguration()
                    .map { config -> config.toBuilder().addApiName(getApiName()).build() }
                    .orElseGet { AwsRequestOverrideConfiguration.builder().addApiName(getApiName()).build() }

            return request.toBuilder().overrideConfiguration(overrideConfiguration).build()
        }

        private fun getApiName(): ApiName = ApiName
            .builder()
            .version(MavenPackaging.VERSION)
            .name(buildUserAgentSuffix())
            .build()

        private fun buildUserAgentSuffix(): String {
            val userAgentSuffixItems =
                ImmutableMap.of(
                    "autoreg",
                    if (configuration.isSchemaAutoRegistrationEnabled) ONE else ZERO,
                    "compress",
                    if (configuration.compressionType == AWSSchemaRegistryConstants.COMPRESSION.ZLIB) ONE else ZERO,
                    "secdeser",
                    if (configuration.secondaryDeserializer != null) ONE else ZERO,
                    "app",
                    configuration.userAgentApp!!,
                )

            val userAgentSuffix = StringJoiner(":")
            userAgentSuffixItems.forEach { (key, value) -> userAgentSuffix.add("$key/$value") }
            return userAgentSuffix.toString()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(AWSSchemaRegistryClient::class.java)
        private const val MAX_ATTEMPTS = 10
        private const val MAX_WAIT_INTERVAL = 3000L
        private const val BASE_RETRY_WAIT_INTERVAL = 100L

        // Held by the outer class companion: a Kotlin `inner` class cannot have a
        // companion object of its own.
        private const val ONE = "1"
        private const val ZERO = "0"
    }
}
