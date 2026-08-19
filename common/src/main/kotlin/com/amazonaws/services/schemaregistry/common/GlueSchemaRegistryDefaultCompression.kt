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
import org.slf4j.LoggerFactory

/**
 * Compresses and Decompresses records using the Zlib algorithm.
 */
class GlueSchemaRegistryDefaultCompression : GlueSchemaRegistryCompressionHandler {
    override fun compress(record: ByteArray?): ByteArray {
        try {
            val compressed = super.compress(record)
            log.debug("Compression :: record length: {}", formatDataLengthInKB(record!!.size))
            log.debug("Compression :: record length after compression: {}", formatDataLengthInKB(compressed.size))
            return compressed
        } catch (e: Exception) {
            val message = "Error while compressing data"
            log.error(message, e)
            throw AWSSchemaRegistryException(message, e)
        }
    }

    override fun decompress(
        compressedRecord: ByteArray?,
        start: Int,
        end: Int,
    ): ByteArray {
        try {
            val deCompressedRecord = super.decompress(compressedRecord, start, end)
            log.debug("Decompression :: Compressed record length: {}", formatDataLengthInKB(compressedRecord!!.size))
            log.debug("Decompression :: Decompressed record length: {}", formatDataLengthInKB(deCompressedRecord.size))
            return deCompressedRecord
        } catch (e: Exception) {
            val message = "Error while decompressing data"
            log.error(message, e)
            throw AWSSchemaRegistryException(message, e)
        }
    }

    private fun formatDataLengthInKB(dataLength: Int): String = "${dataLength / 1024}$KILO_BYTES"

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryDefaultCompression::class.java)
        private const val KILO_BYTES = "KB"
    }
}
