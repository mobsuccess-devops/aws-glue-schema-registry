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

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class JsonDataWithSchemaTest {
    @Test
    fun testInValidJsonDataWithSchema() {
        assertThrows(IllegalArgumentException::class.java) {
            JsonDataWithSchema.builder("", "").build()
        }
        assertThrows(IllegalArgumentException::class.java) {
            JsonDataWithSchema.builder(null, "").build()
        }
        assertThrows(IllegalArgumentException::class.java) {
            JsonDataWithSchema.builder("", null).build()
        }
        assertThrows(IllegalArgumentException::class.java) {
            JsonDataWithSchema.builder(null, null).build()
        }
    }

    @Test
    fun testEmptyValidJsonDataWithSchema() {
        val schema =
            """
            {
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "${'$'}id": "http://example.com/product.schema.json",
              "title": "Product",
              "description": "A product in the catalog",
              "type": "string"
            }
            """.trimIndent()
        val payload = ""
        val jsonDataWithSchema = JsonDataWithSchema.builder(schema, payload).build()
        assertNotNull(jsonDataWithSchema)
    }

    @Test
    fun testNullJsonDataWithSchema() {
        val schema =
            """
            {
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "${'$'}id": "http://example.com/product.schema.json",
              "title": "Product",
              "description": "A product in the catalog",
              "type": "null"
            }
            """.trimIndent()
        val payload: String? = null
        val jsonDataWithSchema = JsonDataWithSchema.builder(schema, payload).build()
        assertNotNull(jsonDataWithSchema)
    }
}
