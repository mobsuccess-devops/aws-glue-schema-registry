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

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificRecord
import java.io.ByteArrayOutputStream
import java.util.concurrent.ExecutionException

/**
 * Avro serialization helper.
 */
// `open`: the test suites mock this type.
open class AvroSerializer : GlueSchemaRegistryDataFormatSerializer {
    private val avroUtils: AVROUtils = AVROUtils.getInstance()

    @JvmField
    @VisibleForTesting
    protected val datumWriterCache: LoadingCache<DatumWriterCacheKey, DatumWriter<Any>> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_DATUM_WRITER_CACHE_SIZE)
            .build(DatumWriterCache())

    override fun serialize(data: Any): ByteArray = encodeData(data, createDatumWriter(data))

    /**
     * Creates the Avro datum writer for serialization. Based on the Avro record type, a
     * GenericDatumWriter or a SpecificDatumWriter is created.
     */
    private fun createDatumWriter(objectToWrite: Any): DatumWriter<Any> {
        val schema = AVROUtils.getInstance().getSchema(objectToWrite)
        return when (objectToWrite) {
            is SpecificRecord -> getSpecificDatumWriter(schema!!)
            is GenericRecord -> getGenericDatumWriter(schema!!)
            is GenericData.EnumSymbol -> getGenericDatumWriter(schema!!)
            is GenericData.Array<*> -> getGenericDatumWriter(schema!!)
            is GenericData.Fixed -> getGenericDatumWriter(schema!!)
            else -> throw AWSSchemaRegistryException("Unsupported type passed for serialization: $objectToWrite")
        }
    }

    private fun getSpecificDatumWriter(schema: Schema): DatumWriter<Any> = try {
        datumWriterCache.get(DatumWriterCacheKey(schema, AvroRecordType.SPECIFIC_RECORD))
    } catch (e: ExecutionException) {
        throw AWSSchemaRegistryException("Failed to get SpecificDatumWriter from cache", e.cause)
    }

    private fun getGenericDatumWriter(schema: Schema): DatumWriter<Any> = try {
        datumWriterCache.get(DatumWriterCacheKey(schema, AvroRecordType.GENERIC_RECORD))
    } catch (e: ExecutionException) {
        throw AWSSchemaRegistryException("Failed to get GenericDatumWriter from cache", e.cause)
    }

    private fun encodeData(
        objectToWrite: Any,
        writer: DatumWriter<Any>,
    ): ByteArray {
        val actualDataBytes = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().directBinaryEncoder(actualDataBytes, null)
        try {
            writer.write(objectToWrite, encoder)
            encoder.flush()
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(e.message, e)
        }
        return actualDataBytes.toByteArray()
    }

    /**
     * Get the schema definition.
     */
    override fun getSchemaDefinition(objectToSerialize: Any): String = avroUtils.getSchemaDefinition(objectToSerialize)

    override fun validate(data: Any) {
        // No-op.
        // Avro format assumes that the passed object contains schema and data that are mutually
        // conformant. We cannot validate the data against the schema.
    }

    override fun validate(
        schemaDefinition: String,
        data: ByteArray,
    ) {
        // No-op.
        // We cannot determine accurately if the data bytes match the schema, as Avro bytes do not
        // carry the field names.
    }

    // Visible rather than private: it is a type argument of the protected cache field, which Kotlin
    // refuses to expose from a private type where Java's erasure tolerated it.
    data class DatumWriterCacheKey(
        val schema: Schema,
        val avroRecordType: AvroRecordType,
    )

    private class DatumWriterCache : CacheLoader<DatumWriterCacheKey, DatumWriter<Any>>() {
        override fun load(datumWriterCacheKey: DatumWriterCacheKey): DatumWriter<Any> = DatumWriterInstance.get(datumWriterCacheKey.schema, datumWriterCacheKey.avroRecordType)
    }

    companion object {
        private const val MAX_DATUM_WRITER_CACHE_SIZE = 100L
    }
}
