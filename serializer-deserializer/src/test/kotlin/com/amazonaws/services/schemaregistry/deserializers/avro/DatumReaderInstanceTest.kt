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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.SpecificDatumReader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Paths

class DatumReaderInstanceTest {
    @Test
    fun testFrom_specificRecordWithGeneratedClass_returnsSpecificDatumReader() {
        val reader = DatumReaderInstance.from(userSchemaDefinition(), AvroRecordType.SPECIFIC_RECORD)

        assertInstanceOf(SpecificDatumReader::class.java, reader)
    }

    @Test
    fun testFrom_genericRecord_returnsGenericDatumReader() {
        val reader = DatumReaderInstance.from(empSchemaDefinition(), AvroRecordType.GENERIC_RECORD)

        assertInstanceOf(GenericDatumReader::class.java, reader)
    }

    @Test
    fun testFrom_specificRecordWithoutGeneratedClass_throwsNamedException() {
        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                DatumReaderInstance.from(empSchemaDefinition(), AvroRecordType.SPECIFIC_RECORD)
            }

        assertTrue(
            exception.message!!.contains("com.amazonaws.services.schemaregistry.serializers.avro.emp"),
            "Message must name the schema whose class is missing: ${exception.message}",
        )
        assertTrue(
            exception.message!!.contains("classpath"),
            "Message must point at the classpath: ${exception.message}",
        )
        assertTrue(
            exception.message!!.contains("avroRecordType") && exception.message!!.contains("GENERIC_RECORD"),
            "Message must name the way out: ${exception.message}",
        )
    }

    @Test
    fun testFrom_unknownRecordType_throwsUnsupportedOperation() {
        val exception =
            assertThrows(UnsupportedOperationException::class.java) {
                DatumReaderInstance.from(userSchemaDefinition(), AvroRecordType.UNKNOWN)
            }

        assertEquals("Unsupported AvroRecordType: UNKNOWN", exception.message)
    }

    private fun userSchemaDefinition(): String = readSchema("src/test/resources/avro/user.avsc")

    private fun empSchemaDefinition(): String = readSchema("src/test/resources/avro/emp_record.avsc")

    private fun readSchema(path: String): String = String(Files.readAllBytes(Paths.get(path)))
}
