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

package com.amazonaws.services.schemaregistry.deserializers.protobuf

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.protobuf.Descriptors
import java.nio.ByteBuffer

// `open`: the test suites mock this type.
open class ProtobufDeserializer(
    configs: GlueSchemaRegistryConfiguration,
) : GlueSchemaRegistryDataFormatDeserializer {
    private val protoDecoder = ProtobufWireFormatDecoder(MessageIndexFinder())
    private val protobufMessageType: ProtobufMessageType? = configs.protobufMessageType

    @JvmField
    @VisibleForTesting
    internal val schemaParserCache: LoadingCache<ProtobufSchemaParserCacheKey, Descriptors.FileDescriptor> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_PROTOBUF_SCHEMA_PARSER_CACHE_SIZE)
            .build(ProtobufSchemaParserCache())

    override fun deserialize(
        data: ByteBuffer,
        schema: Schema,
    ): Any {
        try {
            val plainData = DESERIALIZER_DATA_PARSER.getPlainData(data)
            val protoFileName = getProtoFileName(schema.schemaName)

            val fileDescriptor =
                schemaParserCache.get(ProtobufSchemaParserCacheKey(schema.schemaDefinition, protoFileName))

            return protoDecoder.decode(plainData, fileDescriptor, protobufMessageType)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("Exception occurred while de-serializing Protobuf message", e)
        }
    }

    /**
     * The schema name doubles as the proto file name. During schema creation users are expected to
     * name the schema after the proto file; the ".proto" suffix is optional.
     */
    private fun getProtoFileName(schemaName: String): String {
        val protoExtension = ".proto"
        val extensionIndex = schemaName.lastIndexOf(protoExtension)

        // Extension absent, append it. Ex: Basic -> Basic.proto
        if (extensionIndex == -1) {
            return schemaName + protoExtension
        }

        // Extension already at the end, return as is. Ex: basic.proto -> basic.proto
        if (extensionIndex + protoExtension.length == schemaName.length) {
            return schemaName
        }

        // Extension not at the end, append it. Ex: basic.protofoo.schema -> basic.protofoo.schema.proto
        return schemaName + protoExtension
    }

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class ProtobufDeserializerBuilder internal constructor() {
        private var configs: GlueSchemaRegistryConfiguration? = null

        fun configs(configs: GlueSchemaRegistryConfiguration?): ProtobufDeserializerBuilder = apply { this.configs = configs }

        fun build(): ProtobufDeserializer = ProtobufDeserializer(configs!!)
    }

    // Visible rather than private: it is a type argument of the protected cache field, which
    // Kotlin refuses to expose otherwise. Java's erasure tolerated the private nested class.
    data class ProtobufSchemaParserCacheKey(
        val schemaDefinition: String,
        val protoFileName: String,
    )

    private class ProtobufSchemaParserCache : CacheLoader<ProtobufSchemaParserCacheKey, Descriptors.FileDescriptor>() {
        @Throws(Exception::class)
        override fun load(cacheKey: ProtobufSchemaParserCacheKey): Descriptors.FileDescriptor = ProtobufSchemaParser.parse(cacheKey.schemaDefinition, cacheKey.protoFileName)
    }

    companion object {
        private val DESERIALIZER_DATA_PARSER = GlueSchemaRegistryDeserializerDataParser.getInstance()

        // Make this configurable if required.
        private const val MAX_PROTOBUF_SCHEMA_PARSER_CACHE_SIZE = 100L

        @JvmStatic
        fun builder(): ProtobufDeserializerBuilder = ProtobufDeserializerBuilder()
    }
}
