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

package com.amazonaws.services.schemaregistry.kafkaconnect

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Integration test to verify the secondary deserializer fix works end-to-end.
 * This test simulates the real-world scenario where Confluent data is processed
 * by a GSR converter with a secondary deserializer configured.
 */
class AWSKafkaAvroConverterIntegrationTest {
    private lateinit var converter: AWSKafkaAvroConverter

    @BeforeEach
    fun setUp() {
        converter = AWSKafkaAvroConverter()

        val configs = HashMap<String, Any>()
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()

        // Configure secondary deserializer for non-GSR data
        configs[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER] = StringDeserializer::class.java.name

        converter.configure(configs, false)
    }

    /**
     * Integration test that verifies the fix handles non-GSR data correctly.
     * This test creates a real GenericRecord and verifies that schema extraction
     * works properly when the data cannot be deserialized by GSR.
     */
    @Test
    fun testSecondaryDeserializerSchemaExtraction_RealAvroData() {
        // Create a real Avro record
        val avroRecord: GenericRecord = GenericData.Record(AVRO_SCHEMA)
        avroRecord.put("name", "John Doe")
        avroRecord.put("age", 30)

        // Test the schema extraction method directly
        val extractedSchema = converter.extractSchemaFromAvroObject(avroRecord)

        // Verify the schema was extracted correctly
        assertNotNull(extractedSchema)
        assertEquals(AVRO_SCHEMA, extractedSchema)
        assertEquals("User", extractedSchema.name)
        assertEquals(2, extractedSchema.fields.size)
        assertEquals("name", extractedSchema.fields[0].name())
        assertEquals("age", extractedSchema.fields[1].name())
    }

    /**
     * Test that verifies extractAvroSchema method correctly routes to object-based
     * schema extraction for non-GSR data.
     */
    @Test
    fun testExtractAvroSchema_NonGSRData_UsesObjectExtraction() {
        // Create a real Avro record
        val avroRecord: GenericRecord = GenericData.Record(AVRO_SCHEMA)
        avroRecord.put("name", "Jane Smith")
        avroRecord.put("age", 25)

        // Simulate non-GSR data
        val nonGSRData = byteArrayOf(0, 0, 0, 0, 1, 65)

        // Test the schema extraction
        val extractedSchema = converter.extractAvroSchema(nonGSRData, avroRecord)

        // Verify the schema was extracted from the object, not the bytes
        assertNotNull(extractedSchema)
        assertEquals(AVRO_SCHEMA, extractedSchema)
    }

    /**
     * Test error handling for invalid Avro objects.
     */
    @Test
    fun testExtractSchemaFromAvroObject_InvalidObject_ThrowsException() {
        val invalidObject = "not-an-avro-record"
        val someData = byteArrayOf(1, 2, 3)

        assertThrows(Exception::class.java) {
            converter.extractAvroSchema(someData, invalidObject)
        }
    }

    /**
     * Test that null objects are handled gracefully.
     */
    @Test
    fun testExtractSchemaFromAvroObject_NullObject_ThrowsException() {
        val someData = byteArrayOf(1, 2, 3)

        assertThrows(Exception::class.java) {
            converter.extractAvroSchema(someData, null)
        }
    }

    companion object {
        private const val AVRO_SCHEMA_STRING =
            """{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"""
        private val AVRO_SCHEMA: Schema = Schema.Parser().parse(AVRO_SCHEMA_STRING)
    }
}
