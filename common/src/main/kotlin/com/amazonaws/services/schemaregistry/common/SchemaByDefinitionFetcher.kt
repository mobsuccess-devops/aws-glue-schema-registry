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
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Fetches the schema version for the given schema definition optionally registering the schema if required.
 */
// `open` : les classes Kotlin sont finales par défaut, contrairement aux classes Java,
// et plusieurs suites de tests mockent ce type.
open class SchemaByDefinitionFetcher(
    private val awsSchemaRegistryClient: AWSSchemaRegistryClient,
    private val glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
) {
    // @JvmField conserve l'accès direct au champ depuis les tests Java du paquet.
    @JvmField
    @VisibleForTesting
    protected val schemaDefinitionToVersionCache: LoadingCache<Schema, UUID> =
        CacheBuilder
            .newBuilder()
            .maximumSize(glueSchemaRegistryConfiguration.cacheSize.toLong())
            .refreshAfterWrite(glueSchemaRegistryConfiguration.timeToLiveMillis, TimeUnit.MILLISECONDS)
            .build(SchemaDefinitionToVersionCache())

    /**
     * Get Schema Version ID by following below steps :
     *
     * 1) If schema version id exists in registry then get it from registry
     * 2) If schema version id does not exist in registry, and auto registration is enabled,
     *    register the schema version, creating the schema first when it does not exist.
     *
     * @throws AWSSchemaRegistryException on any error while fetching the schema version ID
     */
    open fun getORRegisterSchemaVersionId(
        schemaDefinition: String,
        schemaName: String,
        dataFormat: String,
        metadata: Map<String, String>,
    ): UUID {
        val schema = Schema(schemaDefinition, dataFormat, schemaName)

        try {
            return schemaDefinitionToVersionCache.get(schema)
        } catch (ex: Exception) {
            // Comme dans le code d'origine, une cause absente déclenche un
            // NullPointerException plutôt qu'un message par défaut.
            val schemaRegistryException = ex.cause!!
            val exceptionCauseMessage =
                if (schemaRegistryException.cause != null) {
                    schemaRegistryException.cause!!.message
                } else {
                    schemaRegistryException.message
                }!!

            val schemaVersionId =
                when {
                    exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG) -> {
                        requireAutoRegistrationEnabled(schemaRegistryException)
                        awsSchemaRegistryClient.registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata)
                    }

                    exceptionCauseMessage.contains(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG) -> {
                        requireAutoRegistrationEnabled(schemaRegistryException)
                        awsSchemaRegistryClient.createSchema(schemaName, dataFormat, schemaDefinition, metadata)
                    }

                    else -> throw AWSSchemaRegistryException(
                        "Exception occurred while fetching or registering schema definition = " +
                            "$schemaDefinition, schema name = $schemaName. Error: $exceptionCauseMessage",
                        schemaRegistryException,
                    )
                }

            schemaDefinitionToVersionCache.put(schema, schemaVersionId)
            return schemaVersionId
        }
    }

    private fun requireAutoRegistrationEnabled(cause: Throwable) {
        if (!glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled) {
            throw AWSSchemaRegistryException(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG, cause)
        }
    }

    private inner class SchemaDefinitionToVersionCache : CacheLoader<Schema, UUID>() {
        override fun load(schema: Schema): UUID = awsSchemaRegistryClient.getSchemaVersionIdByDefinition(
            schema.schemaDefinition,
            schema.schemaName,
            schema.dataFormat,
        )
    }
}
