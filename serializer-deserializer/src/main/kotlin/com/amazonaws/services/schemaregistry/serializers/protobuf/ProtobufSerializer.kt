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

package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.ProtobufSchemaParser
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Message

/**
 * Protobuf serialization helper.
 * This class is instantiated by GlueSchemaRegistryFacade to serialize Protobuf-type objects.
 */
// `open`: the test suites mock this type.
open class ProtobufSerializer(
    configs: GlueSchemaRegistryConfiguration?,
) : GlueSchemaRegistryDataFormatSerializer {
    private val schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration? = configs
    private val protoEncoder = ProtobufWireFormatEncoder(MessageIndexFinder())

    @JvmField
    @VisibleForTesting
    internal val schemaGeneratorCache: LoadingCache<DescriptorProtos.FileDescriptorProto, String> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_SCHEMA_GENERATOR_CACHE)
            .build(SchemaGeneratorCache())

    /**
     * Serialize the Protobuf object to bytes.
     *
     * @throws AWSSchemaRegistryException AWS Schema Registry Exception
     */
    override fun serialize(data: Any): ByteArray {
        validate(data)
        try {
            val protobufMessage = data as Message
            return protoEncoder.encode(protobufMessage, protobufMessage.descriptorForType.file)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("Could not serialize from the type provided", e)
        }
    }

    /**
     * Get the schema definition.
     */
    override fun getSchemaDefinition(objectToSerialize: Any): String {
        validate(objectToSerialize)
        try {
            val message = objectToSerialize as Message
            val fileDescriptorProto = message.descriptorForType.file.toProto()
            return schemaGeneratorCache.get(fileDescriptorProto)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("Could not generate schema from the type provided", e)
        }
    }

    override fun validate(
        schemaDefinition: String,
        data: ByteArray,
    ) {
        // TODO: Implement
        // Left blank as the schema string representation has not been solidified
    }

    override fun validate(data: Any) {
        if (data !is Message) {
            throw AWSSchemaRegistryException("Object is not of Message type: ${data.javaClass}")
        }
    }

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class ProtobufSerializerBuilder internal constructor() {
        private var configs: GlueSchemaRegistryConfiguration? = null

        fun configs(configs: GlueSchemaRegistryConfiguration?): ProtobufSerializerBuilder = apply { this.configs = configs }

        fun build(): ProtobufSerializer = ProtobufSerializer(configs)
    }

    private class SchemaGeneratorCache : CacheLoader<DescriptorProtos.FileDescriptorProto, String>() {
        override fun load(fileDescriptorProto: DescriptorProtos.FileDescriptorProto): String = ProtobufSchemaParser.getProtobufSchemaStringFromFileDescriptorProto(fileDescriptorProto)
    }

    companion object {
        // Make this configurable if requested by customers.
        private const val MAX_SCHEMA_GENERATOR_CACHE = 100L

        @JvmStatic
        fun builder(): ProtobufSerializerBuilder = ProtobufSerializerBuilder()
    }
}
