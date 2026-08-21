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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import java.io.IOException
import java.io.InputStream

/**
 * AWS Glue Schema Registry input stream de-serializer: extracts the schema from the input stream
 * and strips the schema registry information from it.
 */
// `open`: the test suites mock this type.
open class GlueSchemaRegistryInputStreamDeserializer(
    private val glueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade,
) {
    @VisibleForTesting
    internal val parsedSchemaCache: Cache<String, Schema> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_PARSED_SCHEMA_CACHE_SIZE)
            .build()

    /**
     * Constructor accepting the configuration map for the AWS deserializer.
     */
    constructor(configs: Map<String, Any>) : this(
        GlueSchemaRegistryDeserializationFacade(
            GlueSchemaRegistryConfiguration(configs).apply { userAgentApp = UserAgents.FLINK },
            DefaultCredentialsProvider.builder().build(),
        ),
    )

    /**
     * Get the schema and remove the extra Schema Registry information from the input stream.
     */
    @Throws(IOException::class)
    open fun getSchemaAndDeserializedStream(inputStream: InputStream): Schema {
        val inputBytes = ByteArray(inputStream.available())
        inputStream.read(inputBytes)
        inputStream.reset()

        val mutableByteArrayInputStream = inputStream as MutableByteArrayInputStream
        val schemaDefinition = glueSchemaRegistryDeserializationFacade.getSchema(inputBytes).schemaDefinition
        mutableByteArrayInputStream.setBuffer(glueSchemaRegistryDeserializationFacade.getActualData(inputBytes))

        parsedSchemaCache.getIfPresent(schemaDefinition)?.let { return it }

        return try {
            Schema.Parser().parse(schemaDefinition).also { parsedSchemaCache.put(schemaDefinition, it) }
        } catch (e: SchemaParseException) {
            throw AWSSchemaRegistryException("Error occurred while parsing schema, see inner exception for details.", e)
        }
    }

    private companion object {
        private const val MAX_PARSED_SCHEMA_CACHE_SIZE = 100L
    }
}
