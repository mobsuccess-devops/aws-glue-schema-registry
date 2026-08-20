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

package com.amazonaws.services.schemaregistry.deserializers.avro

import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/**
 * Unit tests for AvroRecordType enumeration
 */
class AvroRecordTypeTest {
    /**
     * Test fromName for existing enum value.
     */
    @Test
    fun test_Existing_Enum_Value() {
        assertEquals(AvroRecordType.GENERIC_RECORD, AvroRecordType.fromName("GENERIC_RECORD"))
        assertEquals(AvroRecordType.SPECIFIC_RECORD, AvroRecordType.fromName("SPECIFIC_RECORD"))
    }

    /**
     * Test fromName for non-existent enum value.
     */
    @Test
    fun test_Non_Existent_Enum_Value() {
        assertThrows(IllegalArgumentException::class.java) { AvroRecordType.fromName("SomeRandomVal") }
    }

    /**
     * Test fromName for empty enum value.
     */
    @Test
    fun test_Null_Enum_Value() {
        assertThrows(IllegalArgumentException::class.java) { AvroRecordType.fromName("") }
    }

    /**
     * Test getName method.
     */
    @Test
    fun test_GetName() {
        assertEquals("SPECIFIC_RECORD", AvroRecordType.SPECIFIC_RECORD.getName())
    }

    /**
     * Test getValue method.
     */
    @Test
    fun test_GetValue() {
        assertEquals(1, AvroRecordType.SPECIFIC_RECORD.value)
    }
}
