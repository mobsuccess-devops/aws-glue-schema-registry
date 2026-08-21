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

package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.DataFormatException
import java.util.zip.Deflater
import java.util.zip.Inflater

interface GlueSchemaRegistryCompressionHandler {
    /**
     * Compresses the record.
     *
     * @return compressed byte array representation.
     */
    @Throws(IOException::class)
    fun compress(record: ByteArray?): ByteArray {
        // The parameter stays nullable: the Java code accepted null and let the
        // NullPointerException surface from *inside* the implementation's try block,
        // which wraps it into an AWSSchemaRegistryException. A non-nullable type would
        // trip the check before the try is entered.
        val bytes = record!!
        val deflator = getDeflatorObject(bytes)
        val compressed = writeToDeflatorObject(bytes, deflator)
        deflator.end()
        return compressed
    }

    /**
     * Need to provide the start bit and end bit for decompressor to decompress the
     * specified bits in the byte array.
     *
     * @return decompressed byte array.
     */
    @Throws(IOException::class)
    fun decompress(
        compressedRecord: ByteArray?,
        start: Int,
        end: Int,
    ): ByteArray {
        val bytes = compressedRecord!!
        val inflator = getInflatorObject(bytes, start, end)
        val decompressed = decompress(inflator, bytes.size)
        inflator.end()
        return decompressed
    }

    companion object {
        const val BUFFER_SIZE = 1024

        @Throws(IOException::class)
        fun writeToDeflatorObject(
            record: ByteArray,
            deflater: Deflater,
        ): ByteArray {
            val outputStream = ByteArrayOutputStream(record.size)
            val buffer = ByteArray(BUFFER_SIZE)
            while (!deflater.finished()) {
                val count = deflater.deflate(buffer)
                outputStream.write(buffer, 0, count)
            }
            outputStream.close()
            return outputStream.toByteArray()
        }

        fun getDeflatorObject(record: ByteArray): Deflater {
            val deflater = Deflater()
            deflater.setInput(record)
            deflater.finish()
            return deflater
        }

        fun getInflatorObject(
            compressedRecord: ByteArray,
            start: Int,
            end: Int,
        ): Inflater {
            val inflater = Inflater()
            inflater.setInput(compressedRecord, start, end)
            return inflater
        }

        @Throws(IOException::class)
        fun decompress(
            inflater: Inflater,
            size: Int,
        ): ByteArray = writeToByteArrayOutputStream(inflater, ByteArrayOutputStream(size))

        @Throws(IOException::class)
        fun writeToByteArrayOutputStream(
            inflater: Inflater,
            outputStream: ByteArrayOutputStream,
        ): ByteArray {
            val buffer = ByteArray(BUFFER_SIZE)
            while (!inflater.finished()) {
                val count: Int =
                    try {
                        inflater.inflate(buffer)
                    } catch (e: DataFormatException) {
                        throw AWSSchemaRegistryException("Bytes received is not compressed properly", e)
                    }
                if (count == 0 && inflater.needsDictionary()) {
                    throw AWSSchemaRegistryException(
                        "Compressed bytes need a preset dictionary, which the schema registry format does not carry",
                    )
                }
                if (count == 0 && inflater.needsInput()) {
                    throw AWSSchemaRegistryException(
                        "Compressed bytes are truncated: the stream ends before the decompressed data does",
                    )
                }
                outputStream.write(buffer, 0, count)
            }
            outputStream.close()
            return outputStream.toByteArray()
        }
    }
}
