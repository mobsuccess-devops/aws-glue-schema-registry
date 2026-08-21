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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets

class JsonSerializerTest {
    private val jsonSerializer =
        JsonSerializer(
            GlueSchemaRegistryConfiguration(hashMapOf(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2")),
        )

    private val nullableJsonSerializer =
        JsonSerializer(
            GlueSchemaRegistryConfiguration(
                hashMapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
                    AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED to true,
                ),
            ),
        )

    @Test
    fun testWrapper_serializeWithGenericRecord_bytesMatch() {
        val jsonPayload = """{"latitude":48.858093,"longitude":2.294694}"""
        val expectedBytes = jsonPayload.toByteArray(StandardCharsets.UTF_8)
        val serializedBytes = jsonSerializer.serialize(GENERIC_TEST_RECORD)
        assertArrayEquals(expectedBytes, serializedBytes)
    }

    @Test
    fun testWrapper_serializeWithSpecificRecord_bytesMatch() {
        val objectMapper = ObjectMapper()
        val expectedBytes = objectMapper.writeValueAsBytes(SPECIFIC_TEST_RECORD)
        val serializedBytes = jsonSerializer.serialize(SPECIFIC_TEST_RECORD)
        assertArrayEquals(expectedBytes, serializedBytes)
    }

    @Test
    fun testValidate_validatesWrapper_successfully() {
        assertDoesNotThrow { jsonSerializer.validate(GENERIC_TEST_RECORD) }
    }

    @Test
    fun testValidate_validatesSpecificRecord_successfully() {
        assertDoesNotThrow { jsonSerializer.validate(SPECIFIC_TEST_RECORD) }
    }

    @Test
    fun testValidate_validatesWrapper_ThrowsValidationException() {
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                jsonSerializer.validate(RecordGenerator.createNonSchemaConformantJsonData())
            }

        assertEquals("JSON data validation against schema failed.", ex.message)
    }

    @Test
    fun testValidate_validatesBytes_successfully() {
        val dataBytes = GENERIC_TEST_RECORD.payload!!.toByteArray()
        val schemaDefinition = GENERIC_TEST_RECORD.schema!!

        assertDoesNotThrow { jsonSerializer.validate(schemaDefinition, dataBytes) }
    }

    @Test
    fun testValidate_validatesBytes_ThrowsValidationException() {
        val nonSchemaConformantRecord = RecordGenerator.createNonSchemaConformantJsonData()
        val dataBytes = nonSchemaConformantRecord.payload!!.toByteArray()
        val schemaDefinition = nonSchemaConformantRecord.schema!!

        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                jsonSerializer.validate(schemaDefinition, dataBytes)
            }

        assertEquals("JSON data validation against schema failed.", ex.message)
    }

    @Test
    fun testValidate_validatesBytes_NonUTF8EncodingThrowsException() {
        // Encoding as UTF-16LE bytes.
        val dataBytes = GENERIC_TEST_RECORD.payload!!.toByteArray(StandardCharsets.UTF_16LE)
        val schemaDefinition = GENERIC_TEST_RECORD.schema!!

        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                jsonSerializer.validate(schemaDefinition, dataBytes)
            }

        assertEquals("Malformed JSON", ex.message)
    }

    @Test
    fun testWrapper_getSchemaDefinition_matches() {
        val schemaDefinition =
            """{"${'$'}id":"https://example.com/geographical-location.schema.json",""" +
                """"${'$'}schema":"http://json-schema.org/draft-07/schema#","title":"Longitude """ +
                """and Latitude Values","description":"A geographical coordinate.",""" +
                """"required":["latitude","longitude"],"type":"object",""" +
                """"properties":{"latitude":{"type":"number","minimum":-90,""" +
                """"maximum":90},"longitude":{"type":"number","minimum":-180,""" +
                """"maximum":180}},"additionalProperties":false}"""
        assertEquals(schemaDefinition, jsonSerializer.getSchemaDefinition(GENERIC_TEST_RECORD))
    }

    @Test
    fun testPojo_getSchemaDefinition_asExpected() {
        val schemaDefinition =
            """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","title":"Simple Car """ +
                """Schema","type":"object","additionalProperties":false,""" +
                """"description":"This is a car","className":"com.amazonaws.services""" +
                """.schemaregistry.serializers.json.Car",""" +
                """"properties":{"make":{"type":"string"},"model":{"type":"string"},""" +
                """"used":{"type":"boolean","default":true},""" +
                """"miles":{"type":"integer","maximum":200000,"multipleOf":1000},""" +
                """"year":{"type":"integer","minimum":2000},""" +
                """"purchaseDate":{"type":"integer","format":"utc-millisec"},""" +
                """"listedDate":{"type":"integer","format":"utc-millisec"},""" +
                """"owners":{"type":"array","items":{"type":"string"}},""" +
                """"serviceChecks":{"type":"array","items":{"type":"number"}}},""" +
                """"required":["make","model","used","miles","year"]}"""
        assertEquals(schemaDefinition, jsonSerializer.getSchemaDefinition(SPECIFIC_TEST_RECORD))
    }

    @Test
    fun testGetSchemaDefinition_nullObject_throwsException() {
        assertThrows(NullPointerException::class.java) { jsonSerializer.getSchemaDefinition(nullOf()) }
    }

    @Test
    fun testSerialize_nullObject_throwsException() {
        assertThrows(NullPointerException::class.java) { jsonSerializer.serialize(nullOf()) }
    }

    @Test
    fun testGetSchemaDefinition_withoutTheNullableSetting_typesAnOptionalFieldDirectly() {
        val schema = ObjectMapper().readTree(jsonSerializer.getSchemaDefinition(SPECIFIC_TEST_RECORD))

        val purchaseDate = schema.path("properties").path("purchaseDate")
        assertEquals("integer", purchaseDate.path("type").asText())
        assertTrue(purchaseDate.path("oneOf").isMissingNode)
    }

    @Test
    fun testGetSchemaDefinition_withTheNullableSetting_offersNullAsWellAsTheType() {
        val schema = ObjectMapper().readTree(nullableJsonSerializer.getSchemaDefinition(SPECIFIC_TEST_RECORD))

        val branches = schema.path("properties").path("purchaseDate").path("oneOf")
        assertEquals(2, branches.size())
        assertTrue(branches.any { it.path("type").asText() == "null" })
        assertTrue(branches.any { it.path("type").asText() == "integer" })
    }

    @Test
    fun testGetSchemaDefinition_withTheNullableSetting_leavesARequiredFieldAlone() {
        val schema = ObjectMapper().readTree(nullableJsonSerializer.getSchemaDefinition(SPECIFIC_TEST_RECORD))

        assertEquals("string", schema.path("properties").path("make").path("type").asText())
    }

    @Test
    fun testSerialize_withTheNullableSetting_acceptsANullOptionalField() {
        val car = Car.builder().make("Tesla").model("Model 3").used(true).miles(1000).year(2020).build()

        assertThrows(AWSSchemaRegistryException::class.java) { jsonSerializer.serialize(car) }
        assertDoesNotThrow { nullableJsonSerializer.serialize(car) }
    }

    companion object {
        private val GENERIC_TEST_RECORD: JsonDataWithSchema =
            RecordGenerator.createGenericJsonRecord(RecordGenerator.TestJsonRecord.GEOLOCATION)
        private val SPECIFIC_TEST_RECORD = RecordGenerator.createSpecificJsonRecord()
    }
}
