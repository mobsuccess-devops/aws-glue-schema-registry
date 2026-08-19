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
import org.apache.flink.formats.avro.SchemaCoder
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

/**
 * Schema coder that allows reading a schema embedded into a serialized record.
 * Used by [GlueSchemaRegistryAvroDeserializationSchema] and [GlueSchemaRegistryAvroSerializationSchema].
 */
class GlueSchemaRegistryAvroSchemaCoder : SchemaCoder {
    private var glueSchemaRegistryInputStreamDeserializer: GlueSchemaRegistryInputStreamDeserializer? = null
    private var glueSchemaRegistryOutputStreamSerializer: GlueSchemaRegistryOutputStreamSerializer? = null

    /**
     * Constructor accepting the transport name and the configuration map.
     */
    constructor(transportName: String?, configs: Map<String, Any>) {
        glueSchemaRegistryInputStreamDeserializer = GlueSchemaRegistryInputStreamDeserializer(configs)
        glueSchemaRegistryOutputStreamSerializer = GlueSchemaRegistryOutputStreamSerializer(transportName, configs)
    }

    @VisibleForTesting
    protected constructor(glueSchemaRegistryInputStreamDeserializer: GlueSchemaRegistryInputStreamDeserializer) {
        this.glueSchemaRegistryInputStreamDeserializer = glueSchemaRegistryInputStreamDeserializer
    }

    @VisibleForTesting
    protected constructor(glueSchemaRegistryOutputStreamSerializer: GlueSchemaRegistryOutputStreamSerializer) {
        this.glueSchemaRegistryOutputStreamSerializer = glueSchemaRegistryOutputStreamSerializer
    }

    @Throws(IOException::class)
    override fun readSchema(inputStream: InputStream): Schema =
        glueSchemaRegistryInputStreamDeserializer!!.getSchemaAndDeserializedStream(inputStream)

    @Throws(IOException::class)
    override fun writeSchema(
        schema: Schema,
        out: OutputStream,
    ) {
        val byteArrayOutputStream = out as ByteArrayOutputStream
        val data = byteArrayOutputStream.toByteArray()
        byteArrayOutputStream.reset()
        glueSchemaRegistryOutputStreamSerializer!!.registerSchemaAndSerializeStream(schema, out, data)
    }
}
