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
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import software.amazon.awssdk.arns.Arn
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Protocol agnostic AWS Generic de-serializer.
 */
// `open`: the test suites mock this type.
open class GlueSchemaRegistryDeserializationFacade : Closeable {
    val credentialsProvider: AwsCredentialsProvider
    val schemaRegistryClient: AWSSchemaRegistryClient
    val glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration

    var deserializerFactory: GlueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()

    @JvmField
    @VisibleForTesting
    internal var cache: LoadingCache<UUID, Schema>

    /**
     * Constructor accepting various dependencies.
     */
    constructor(
        configs: Map<String, *>?,
        properties: Properties?,
        credentialProvider: AwsCredentialsProvider,
        schemaRegistryClient: AWSSchemaRegistryClient?,
    ) {
        credentialsProvider = credentialProvider
        glueSchemaRegistryConfiguration =
            when {
                configs != null -> GlueSchemaRegistryConfiguration(configs)
                properties != null -> GlueSchemaRegistryConfiguration(properties)
                else -> throw AWSSchemaRegistryException("Either properties or configuration has to be provided")
            }
        this.schemaRegistryClient =
            schemaRegistryClient ?: AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration)
        cache = initializeCache()
    }

    constructor(
        configuration: GlueSchemaRegistryConfiguration,
        credentialsProvider: AwsCredentialsProvider,
    ) {
        this.credentialsProvider = credentialsProvider
        glueSchemaRegistryConfiguration = configuration
        schemaRegistryClient = AWSSchemaRegistryClient(credentialsProvider, glueSchemaRegistryConfiguration)
        cache = initializeCache()
    }

    private fun initializeCache(): LoadingCache<UUID, Schema> = CacheBuilder
        .newBuilder()
        .maximumSize(glueSchemaRegistryConfiguration.cacheSize.toLong())
        .refreshAfterWrite(glueSchemaRegistryConfiguration.timeToLiveMillis, TimeUnit.MILLISECONDS)
        .build(GlueSchemaRegistryDeserializationCacheLoader())

    /**
     * Overrides the user-agent app name for the de-serializer, replacing the value previously set
     * in GlueSchemaRegistryConfiguration.
     */
    open fun overrideUserAgentApp(name: String?) {
        glueSchemaRegistryConfiguration.userAgentApp = name!!
    }

    /**
     * Fetches the schema definition for the serialized data.
     *
     * @throws GlueSchemaRegistryIncompatibleDataException when data is incompatible with schema registry
     */
    open fun getSchemaDefinition(buffer: ByteBuffer): String = getAwsDeserializerSchema(buffer).schema.schemaDefinition

    open fun getActualData(data: ByteArray): ByteArray = GlueSchemaRegistryDeserializerDataParser.getInstance().getPlainData(ByteBuffer.wrap(data))

    open fun getSchema(data: ByteArray): Schema = getAwsDeserializerSchema(ByteBuffer.wrap(data)).schema

    /**
     * Fetches the schema definition for the serialized data.
     *
     * @throws GlueSchemaRegistryIncompatibleDataException when data is incompatible with schema registry
     */
    open fun getSchemaDefinition(data: ByteArray): String = getSchemaDefinition(ByteBuffer.wrap(data))

    /**
     * De-serializes the given data and returns an Object.
     *
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    open fun deserialize(deserializerInput: AWSDeserializerInput): Any {
        val buffer = deserializerInput.buffer
        val schema = getAwsDeserializerSchema(buffer).schema

        return deserializerFactory
            .getInstance(DataFormat.valueOf(schema.dataFormat), glueSchemaRegistryConfiguration)
            .deserialize(buffer, schema)!!
    }

    /**
     * Returns whether the given data array can be deserialized.
     */
    open fun canDeserialize(data: ByteArray?): Boolean {
        if (data == null) {
            return false
        }
        return GlueSchemaRegistryDeserializerDataParser
            .getInstance()
            .isDataCompatible(ByteBuffer.wrap(data), StringBuilder())
    }

    /**
     * Helper returning the schema version id and the schema registry metadata.
     */
    private fun getAwsDeserializerSchema(buffer: ByteBuffer): AwsDeserializerSchema {
        val dataParser = GlueSchemaRegistryDeserializerDataParser.getInstance()
        val schemaVersionId = dataParser.getSchemaVersionId(buffer)
        return AwsDeserializerSchema(schemaVersionId, retrieveSchemaRegistrySchema(schemaVersionId))
    }

    /**
     * Gets the schema details for the schema version id from the schema registry.
     *
     * @throws AWSSchemaRegistryException when getting the schema by id fails
     */
    private fun retrieveSchemaRegistrySchema(schemaVersionId: UUID): Schema = try {
        cache.get(schemaVersionId)
    } catch (e: Exception) {
        throw AWSSchemaRegistryException(e.cause)
    }

    private fun getSchemaName(schemaArn: String): String {
        val resource = Arn.fromString(schemaArn).resourceAsString()
        return resource.split("/").last()
    }

    /**
     * Resource clean up for Closeable.
     */
    override fun close() {
        // No-op.
    }

    private data class AwsDeserializerSchema(
        val schemaVersionId: UUID,
        val schema: Schema,
    )

    private inner class GlueSchemaRegistryDeserializationCacheLoader : CacheLoader<UUID, Schema>() {
        override fun load(schemaVersionId: UUID): Schema {
            val response = schemaRegistryClient.getSchemaVersionResponse(schemaVersionId.toString())
            return Schema(response.schemaDefinition(), response.dataFormat().name, getSchemaName(response.schemaArn()))
        }
    }

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class GlueSchemaRegistryDeserializationFacadeBuilder internal constructor() {
        private var configs: Map<String, *>? = null
        private var properties: Properties? = null
        private var credentialProvider: AwsCredentialsProvider? = null
        private var schemaRegistryClient: AWSSchemaRegistryClient? = null

        fun configs(configs: Map<String, *>?) = apply { this.configs = configs }

        fun properties(properties: Properties?) = apply { this.properties = properties }

        fun credentialProvider(credentialProvider: AwsCredentialsProvider?) = apply { this.credentialProvider = credentialProvider }

        fun schemaRegistryClient(schemaRegistryClient: AWSSchemaRegistryClient?) = apply { this.schemaRegistryClient = schemaRegistryClient }

        fun build(): GlueSchemaRegistryDeserializationFacade = GlueSchemaRegistryDeserializationFacade(configs, properties, credentialProvider!!, schemaRegistryClient)
    }

    companion object {
        @JvmStatic
        fun builder(): GlueSchemaRegistryDeserializationFacadeBuilder = GlueSchemaRegistryDeserializationFacadeBuilder()
    }
}
