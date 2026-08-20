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

import com.google.common.annotations.VisibleForTesting
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema
import org.apache.flink.formats.avro.SchemaCoder
import java.io.IOException
import java.io.UncheckedIOException

/**
 * AWS Glue Schema Registry Serialization schema to serialize to the Avro binary format for a Flink
 * producer.
 *
 * @param T the type to be serialized
 */
class GlueSchemaRegistryAvroSerializationSchema<T> : RegistryAvroSerializationSchema<T> {
    private constructor(
        recordClazz: Class<T>,
        reader: Schema?,
        schemaCoderProvider: SchemaCoder.SchemaCoderProvider,
    ) : super(recordClazz, reader, schemaCoderProvider)

    @VisibleForTesting
    internal constructor(
        recordClazz: Class<T>,
        reader: Schema?,
        schemaCoder: SchemaCoder,
        // Pass a null schema coder provider.
    ) : super(recordClazz, reader, null) {
        this.schemaCoder = schemaCoder
    }

    /**
     * Serializes the incoming element to a byte array containing AWS Glue Schema Registry
     * information.
     */
    override fun serialize(objectToSerialize: T?): ByteArray? {
        checkAvroInitialized()

        if (objectToSerialize == null) {
            return null
        }
        try {
            val outputStream = getOutputStream()
            outputStream.reset()
            val encoder = getEncoder()
            getDatumWriter().write(objectToSerialize, encoder)
            schemaCoder.writeSchema(getSchema(), outputStream)
            encoder.flush()
            return outputStream.toByteArray()
        } catch (e: IOException) {
            throw UncheckedIOException("Failed to serialize Avro record", e)
        }
    }

    companion object {
        /**
         * Serializes [GenericRecord] using the provided schema.
         */
        @JvmStatic
        fun forGeneric(
            schema: Schema,
            transportName: String?,
            configs: Map<String, Any>,
        ): GlueSchemaRegistryAvroSerializationSchema<GenericRecord> = GlueSchemaRegistryAvroSerializationSchema(
            GenericRecord::class.java,
            schema,
            GlueSchemaRegistryAvroSchemaCoderProvider(transportName, configs),
        )

        /**
         * Serializes [SpecificRecord] using the provided schema.
         */
        @JvmStatic
        fun <T : SpecificRecord> forSpecific(
            clazz: Class<T>,
            transportName: String?,
            configs: Map<String, Any>,
        ): GlueSchemaRegistryAvroSerializationSchema<T> = GlueSchemaRegistryAvroSerializationSchema(
            clazz,
            null,
            GlueSchemaRegistryAvroSchemaCoderProvider(transportName, configs),
        )
    }
}
