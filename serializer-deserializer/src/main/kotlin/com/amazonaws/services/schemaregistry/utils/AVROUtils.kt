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

package com.amazonaws.services.schemaregistry.utils

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.apache.avro.generic.GenericContainer
import org.slf4j.LoggerFactory

class AVROUtils private constructor() {
    /**
     * Get the schema definition.
     */
    fun getSchemaDefinition(objectToDescribe: Any): String {
        val schema =
            getSchema(objectToDescribe) ?: run {
                val message = "Unsupported Type of Record received"
                log.error(message)
                throw AWSSchemaRegistryException(message)
            }
        return schema.toString()
    }

    /**
     * Returns the schema Object, or null when the record type carries none.
     */
    fun getSchema(objectToDescribe: Any): Schema? {
        if (objectToDescribe is GenericContainer) {
            // GenericContainer should contain data of all types except those primitive types
            return objectToDescribe.schema
        }
        log.error("Unsupported Avro Data Formats")
        return null
    }

    /**
     * Parses AVRO Schema from a string.
     */
    fun parseSchema(schema: String): Schema = try {
        Schema.Parser().parse(schema)
    } catch (e: SchemaParseException) {
        throw AWSSchemaRegistryException(
            "Error occurred while parsing schema, see inner exception for details. ",
            e,
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(AVROUtils::class.java)
        private val INSTANCE = AVROUtils()

        /**
         * Thread safe singleton instance of the AVROUtil Class.
         */
        @JvmStatic
        @Synchronized
        fun getInstance(): AVROUtils = INSTANCE
    }
}
