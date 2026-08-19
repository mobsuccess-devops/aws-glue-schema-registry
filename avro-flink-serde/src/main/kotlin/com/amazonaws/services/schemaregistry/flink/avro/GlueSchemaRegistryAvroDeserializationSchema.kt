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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema
import org.apache.flink.formats.avro.SchemaCoder

/**
 * AWS Glue Schema Registry Deserialization schema to de-serialize the Avro binary format for a
 * Flink consumer.
 *
 * @param T type of record it produces
 */
class GlueSchemaRegistryAvroDeserializationSchema<T> private constructor(
    recordClazz: Class<T>,
    reader: Schema?,
    schemaCoderProvider: SchemaCoder.SchemaCoderProvider,
) : RegistryAvroDeserializationSchema<T>(recordClazz, reader, schemaCoderProvider) {
    companion object {
        /**
         * Produces [GenericRecord] using the provided schema.
         */
        @JvmStatic
        fun forGeneric(
            schema: Schema,
            configs: Map<String, Any>,
        ): GlueSchemaRegistryAvroDeserializationSchema<GenericRecord> =
            GlueSchemaRegistryAvroDeserializationSchema(
                GenericRecord::class.java,
                schema,
                GlueSchemaRegistryAvroSchemaCoderProvider(configs),
            )

        /**
         * Produces classes that were generated from an Avro schema.
         */
        @JvmStatic
        fun <T : SpecificRecord> forSpecific(
            clazz: Class<T>,
            configs: Map<String, Any>,
        ): GlueSchemaRegistryAvroDeserializationSchema<T> =
            GlueSchemaRegistryAvroDeserializationSchema(
                clazz,
                null,
                GlueSchemaRegistryAvroSchemaCoderProvider(configs),
            )
    }
}
