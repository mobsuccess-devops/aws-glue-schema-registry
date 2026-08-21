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

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import org.apache.avro.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class AvroSchemaTest {
    @Test
    fun testCanonicalString_withNullSchemaObject_returnsNull() {
        assertNull(AvroSchema(null as Schema?).canonicalString())
    }

    @Test
    fun testFormattedString_withNullSchemaObject_returnsNull() {
        assertNull(AvroSchema(null as Schema?).formattedString(null))
    }

    @Test
    fun testToString_withNullSchemaObject_rendersNull() {
        assertEquals("null", AvroSchema(null as Schema?).toString())
    }

    @Test
    fun testCanonicalString_withParsedSchema_isComputedOnceAndParsesBack() {
        val avroSchema = AvroSchema(SCHEMA_DEFINITION)
        val canonicalString = avroSchema.canonicalString()

        assertNotNull(canonicalString)
        assertEquals(Schema.Parser().parse(SCHEMA_DEFINITION), Schema.Parser().parse(canonicalString!!))
        assertSame(canonicalString, avroSchema.canonicalString())
    }

    companion object {
        private const val SCHEMA_DEFINITION =
            """{"type":"record","name":"User","namespace":"com.example","fields":""" +
                """[{"name":"name","type":"string"}]}"""
    }
}
