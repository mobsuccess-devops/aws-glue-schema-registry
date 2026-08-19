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

package com.amazonaws.services.schemaregistry.serializers

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryCompressionFactory
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryCompressionHandler
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Encodes Schema Register headers into byte buffers.
 */
class SerializationDataEncoder(
    private val schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration,
) {
    private val compressionHandler: GlueSchemaRegistryCompressionHandler? =
        GlueSchemaRegistryCompressionFactory().getCompressionHandler(schemaRegistrySerDeConfigs.compressionType)

    /**
     * Schema Registry header consists of following components:
     * 1. Version byte.
     * 2. Compression byte.
     * 3. Schema Version UUID Id that represents the writer schema.
     * 4. Actual data bytes. The data can be compressed based on configuration.
     */
    fun write(
        objectBytes: ByteArray,
        schemaVersionId: UUID,
    ): ByteArray {
        try {
            ByteArrayOutputStream().use { out ->
                writeHeaderVersionBytes(out)
                writeCompressionBytes(out)
                writeSchemaVersionId(out, schemaVersionId)

                val shouldCompress = compressionHandler != null
                return writeToExistingStream(out, if (shouldCompress) compressData(objectBytes) else objectBytes)
            }
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(e.message, e)
        }
    }

    private fun writeCompressionBytes(out: ByteArrayOutputStream) {
        out.write(
            if (compressionHandler != null) {
                AWSSchemaRegistryConstants.COMPRESSION_BYTE.toInt()
            } else {
                AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE.toInt()
            },
        )
    }

    private fun writeHeaderVersionBytes(out: ByteArrayOutputStream) {
        out.write(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE.toInt())
    }

    @Throws(IOException::class)
    private fun writeSchemaVersionId(
        out: ByteArrayOutputStream,
        schemaVersionId: UUID,
    ) {
        val buffer = ByteBuffer.wrap(ByteArray(AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE))
        buffer.putLong(schemaVersionId.mostSignificantBits)
        buffer.putLong(schemaVersionId.leastSignificantBits)
        out.write(buffer.array())
    }

    @Throws(IOException::class)
    private fun compressData(actualDataBytes: ByteArray): ByteArray = compressionHandler!!.compress(actualDataBytes)

    @Throws(IOException::class)
    private fun writeToExistingStream(
        toStream: ByteArrayOutputStream,
        fromStream: ByteArray,
    ): ByteArray {
        toStream.write(fromStream)
        return toStream.toByteArray()
    }
}
