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

package com.amazonaws.services.schemaregistry.deserializers.avro

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Avro specific de-serializer responsible for handling the Avro protocol
 * specific conversion behavior.
 */
// `open`: the test suites mock this type.
open class AvroDeserializer(
    configs: GlueSchemaRegistryConfiguration,
) : GlueSchemaRegistryDataFormatDeserializer {
    var schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration = configs

    var avroRecordType: AvroRecordType? = configs.avroRecordType

    @JvmField
    @VisibleForTesting
    protected val datumReaderCache: LoadingCache<String, DatumReader<Any>> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_DATUM_READER_CACHE_SIZE)
            .build(DatumReaderCache())

    fun getDatumReaderCache(): LoadingCache<String, DatumReader<Any>> = datumReaderCache

    /**
     * Deserialize the bytes to the original Avro message given the schema retrieved
     * from the schema registry.
     *
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    override fun deserialize(
        data: ByteBuffer,
        schema: Schema,
    ): Any {
        try {
            val schemaDefinition = schema.schemaDefinition
            val plainData = DESERIALIZER_DATA_PARSER.getPlainData(data)

            log.debug("Length of actual message: {}", plainData.size)

            val datumReader = datumReaderCache.get(schemaDefinition)
            val binaryDecoder = getBinaryDecoder(plainData, 0, plainData.size)
            val result = datumReader.read(null, binaryDecoder)

            log.debug("Finished de-serializing Avro message")

            return result
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("Exception occurred while de-serializing Avro message", e)
        }
    }

    private fun getBinaryDecoder(
        data: ByteArray,
        start: Int,
        end: Int,
    ): BinaryDecoder = DecoderFactory.get().binaryDecoder(data, start, end, null)

    private inner class DatumReaderCache : CacheLoader<String, DatumReader<Any>>() {
        @Throws(Exception::class)
        override fun load(schema: String): DatumReader<Any> = DatumReaderInstance.from(schema, avroRecordType!!)
    }

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class AvroDeserializerBuilder internal constructor() {
        private var configs: GlueSchemaRegistryConfiguration? = null

        fun configs(configs: GlueSchemaRegistryConfiguration?): AvroDeserializerBuilder = apply { this.configs = configs }

        fun build(): AvroDeserializer = AvroDeserializer(configs!!)
    }

    companion object {
        private val log = LoggerFactory.getLogger(AvroDeserializer::class.java)
        private val DESERIALIZER_DATA_PARSER = GlueSchemaRegistryDeserializerDataParser.getInstance()

        // TODO: Make this configurable if requested by customers.
        private const val MAX_DATUM_READER_CACHE_SIZE = 100L

        @JvmStatic
        fun builder(): AvroDeserializerBuilder = AvroDeserializerBuilder()
    }
}
