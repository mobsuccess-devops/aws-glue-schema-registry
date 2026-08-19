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

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryCompressionFactory
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Parser that understands the schema registry data format and extracts schema
 * id from serialized data, also performs data integrity validations.
 */
class GlueSchemaRegistryDeserializerDataParser private constructor(
    private val compressionFactory: GlueSchemaRegistryCompressionFactory,
) {
    /**
     * Gets the schema version id embedded within the data.
     *
     * @throws GlueSchemaRegistryIncompatibleDataException when the data is incompatible with
     *                                                     schema registry
     */
    fun getSchemaVersionId(byteBuffer: ByteBuffer): UUID {
        byteBuffer.rewind()
        // Ensure that we are not changing the buffer position.
        val slicedBuffer = byteBuffer.slice()

        // Make sure we have valid byteBuffer.
        validateData(slicedBuffer)

        // Skip HEADER_VERSION_BYTE
        slicedBuffer.get()
        // Skip COMPRESSION_BYTE
        slicedBuffer.get()

        val mostSigBits = slicedBuffer.getLong()
        val leastSigBits = slicedBuffer.getLong()

        return UUID(mostSigBits, leastSigBits)
    }

    /**
     * Validates the data for compatibility with schema registry.
     *
     * @param errorBuilder error message for the validation that can be used by the caller
     * @return true on validation success, false otherwise
     */
    fun isDataCompatible(
        byteBuffer: ByteBuffer,
        errorBuilder: StringBuilder,
    ): Boolean {
        // Ensure that we are not changing the buffer position.
        byteBuffer.rewind()
        val toValidate = byteBuffer.slice()

        // We should be at least 18 bytes long
        if (toValidate.limit() < 18) {
            val message =
                "${GlueSchemaRegistryIncompatibleDataException.UNKNOWN_DATA_ERROR_MESSAGE} size: ${toValidate.limit()}"
            errorBuilder.append(message)
            log.debug(message)
            return false
        }

        val headerVersionByte = toValidate.get()
        if (headerVersionByte != AWSSchemaRegistryConstants.HEADER_VERSION_BYTE) {
            val message = GlueSchemaRegistryIncompatibleDataException.UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE
            errorBuilder.append(message)
            log.debug(message)
            return false
        }

        val compressionByte = toValidate.get()
        if (compressionByte != AWSSchemaRegistryConstants.COMPRESSION_BYTE &&
            compressionByte != AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE
        ) {
            val message = GlueSchemaRegistryIncompatibleDataException.UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE
            errorBuilder.append(message)
            log.debug(message)
            return false
        }

        return true
    }

    fun getPlainData(byteBuffer: ByteBuffer): ByteArray {
        byteBuffer.rewind()
        val slicedBuffer = byteBuffer.slice()

        // Make sure we have the right bytebuffer.
        validateData(slicedBuffer)

        // Seek header byte
        slicedBuffer.get()

        // Seek compression byte.
        val compressionByte = slicedBuffer.get()

        // Seek SchemaVersionId bytes, most then least significant.
        slicedBuffer.getLong()
        slicedBuffer.getLong()

        // Get the actual data.
        val plainData = ByteArray(slicedBuffer.remaining())
        slicedBuffer.get(plainData)

        if (!isCompressionByteSet(compressionByte)) {
            return plainData
        }

        // Decompress the data and return.
        val dataStart = getSchemaRegistryHeaderLength()
        val dataEnd = slicedBuffer.limit() - dataStart
        return decompressData(compressionByte, slicedBuffer, dataStart, dataEnd)
    }

    private fun decompressData(
        compressionByte: Byte,
        compressedData: ByteBuffer,
        start: Int,
        end: Int,
    ): ByteArray {
        try {
            return compressionFactory
                .getCompressionHandler(compressionByte)!!
                .decompress(compressedData.array(), start, end)
        } catch (e: IOException) {
            throw AWSSchemaRegistryException("Failed to decompress data", e)
        }
    }

    /**
     * Helper method for validating the data.
     */
    private fun validateData(buffer: ByteBuffer) {
        val errorMessageBuilder = StringBuilder()
        if (!isDataCompatible(buffer, errorMessageBuilder)) {
            throw GlueSchemaRegistryIncompatibleDataException(errorMessageBuilder.toString())
        }
    }

    /**
     * Whether the byte buffer has been compressed.
     */
    fun isCompressionEnabled(byteBuffer: ByteBuffer): Boolean {
        byteBuffer.rewind()
        val slicedBuffer = byteBuffer.slice()

        // skip the first byte.
        slicedBuffer.get()
        return isCompressionByteSet(slicedBuffer.get())
    }

    private fun isCompressionByteSet(compressionByte: Byte): Boolean = compressionByte != AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE

    /**
     * Get the compression byte.
     */
    fun getCompressionByte(byteBuffer: ByteBuffer): Byte {
        byteBuffer.rewind()
        val slicedBuffer = byteBuffer.slice()
        // skip the first byte.
        slicedBuffer.get()
        return slicedBuffer.get()
    }

    /**
     * Get the header version byte.
     */
    fun getHeaderVersionByte(byteBuffer: ByteBuffer): Byte {
        byteBuffer.rewind()
        return byteBuffer.slice().get()
    }

    private fun getSchemaRegistryHeaderLength(): Int = AWSSchemaRegistryConstants.HEADER_VERSION_BYTE_SIZE +
        AWSSchemaRegistryConstants.COMPRESSION_BYTE_SIZE +
        AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryDeserializerDataParser::class.java)
        private val INSTANCE = GlueSchemaRegistryDeserializerDataParser(GlueSchemaRegistryCompressionFactory())

        /**
         * Singleton instantiation helper.
         */
        @JvmStatic
        fun getInstance(): GlueSchemaRegistryDeserializerDataParser = INSTANCE
    }
}
