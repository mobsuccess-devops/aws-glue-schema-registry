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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants

/**
 * Factory to create the compression object.
 */
class GlueSchemaRegistryCompressionFactory {
    private var zlibCompression: GlueSchemaRegistryDefaultCompression? = null

    /**
     * Get the respective compression handler based on the properties.
     */
    fun getCompressionHandler(compressionType: AWSSchemaRegistryConstants.COMPRESSION?): GlueSchemaRegistryCompressionHandler? {
        if (compressionType != null &&
            AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name.equals(compressionType.name, ignoreCase = true)
        ) {
            return getZlibCompression()
        }
        return null
    }

    /**
     * Return the compression handler based on the byte. A different byte can mean a
     * different compression algorithm implementation.
     */
    fun getCompressionHandler(compressionByte: Byte): GlueSchemaRegistryCompressionHandler? {
        if (AWSSchemaRegistryConstants.COMPRESSION_BYTE == compressionByte) {
            return getZlibCompression()
        }
        return null
    }

    @Synchronized
    private fun getZlibCompression(): GlueSchemaRegistryCompressionHandler {
        if (zlibCompression == null) {
            zlibCompression = GlueSchemaRegistryDefaultCompression()
        }
        return zlibCompression!!
    }
}
