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

package com.amazonaws.services.schemaregistry.serializers.json

import org.apache.commons.lang3.StringUtils

/**
 * Wrapper object that contains schema string and json data string.
 * This works similar to the notion of GenericRecord in Avro, and can be passed as an input
 * to the serializer for the json data format.
 */
class JsonDataWithSchema private constructor(
    /** Json Schema string. */
    val schema: String?,
    /** Json data/payload/document to be serialized. */
    val payload: String?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonDataWithSchema) return false
        return schema == other.schema && payload == other.payload
    }

    override fun hashCode(): Int = 31 * (schema?.hashCode() ?: 0) + (payload?.hashCode() ?: 0)

    override fun toString(): String = "JsonDataWithSchema(schema=$schema, payload=$payload)"

    /** Mirrors the fluent API Lombok generated, including its class name. */
    class JsonDataWithSchemaBuilder internal constructor() {
        private var schema: String? = null
        private var payload: String? = null

        fun schema(schema: String?): JsonDataWithSchemaBuilder = apply { this.schema = schema }

        fun payload(payload: String?): JsonDataWithSchemaBuilder = apply { this.payload = payload }

        fun build(): JsonDataWithSchema = JsonDataWithSchema(schema, payload)
    }

    companion object {
        @JvmStatic
        fun builder(): JsonDataWithSchemaBuilder = JsonDataWithSchemaBuilder()

        /**
         * Builder method validating that a schema was supplied.
         */
        @JvmStatic
        fun builder(
            schema: String?,
            payload: String?,
        ): JsonDataWithSchemaBuilder {
            require(!StringUtils.isBlank(schema)) { "schema can't be blank/empty/null" }
            return JsonDataWithSchemaBuilder().schema(schema).payload(payload)
        }
    }
}
