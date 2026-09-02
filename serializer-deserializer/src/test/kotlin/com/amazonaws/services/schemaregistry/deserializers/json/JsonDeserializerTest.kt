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

package com.amazonaws.services.schemaregistry.deserializers.json

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.json.JsonDeserializer.Companion.MAX_WARNED_CLASS_NAMES
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.json.Car
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class JsonDeserializerTest {
    private val jsonDeserializer = JsonDeserializer(null)

    @Test
    fun testDeserialize_nullArgs_throwsException() {
        val testSchema = Schema(GEOLOCATION_SCHEMA, DataFormat.JSON.name, "testJson")
        val testBytes = GEOLOCATION_PAYLOAD.toByteArray(StandardCharsets.UTF_8)

        assertThrows(NullPointerException::class.java) { jsonDeserializer.deserialize(nullOf(), testSchema) }
        assertThrows(NullPointerException::class.java) {
            jsonDeserializer.deserialize(ByteBuffer.wrap(testBytes), nullOf())
        }
    }

    @Test
    fun testDeserialize_schemaWithoutClassName_returnsJsonDataWithSchema() {
        val schema = Schema(GEOLOCATION_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = jsonDeserializer.deserialize(toSerializedBuffer(GEOLOCATION_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
    }

    @Test
    fun testDeserialize_schemaWithClassName_defaultConfig_returnsJsonDataWithSchema() {
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        // No config supplied (null) -> class name resolution defaults to disabled (secure default),
        // so the schema's className is ignored and a generic JsonDataWithSchema is returned.
        val result = jsonDeserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_schemaWithClassName_resolutionEnabled_returnsSpecificPojo() {
        // Resolution enabled + className in allowlist -> typed POJO
        val deserializer =
            deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.json.Car")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is Car)
    }

    @Test
    fun testDeserialize_schemaWithClassName_resolutionEnabled_noAllowlist_returnsJsonDataWithSchema() {
        // Resolution enabled but no allowlist configured -> falls back to JsonDataWithSchema
        val deserializer = deserializerWithClassNameResolution(true)
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_schemaWithClassName_classNotInAllowlist_returnsJsonDataWithSchema() {
        // Resolution enabled + allowlist configured but className NOT in it -> falls back
        val deserializer = deserializerWithAllowlist("com.example.SomeOtherClass")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_schemaWithClassName_multipleClassesInAllowlist_returnsSpecificPojo() {
        // Multiple classes in allowlist, target is one of them
        val deserializer =
            deserializerWithAllowlist(
                "com.example.Foo, com.amazonaws.services.schemaregistry.serializers.json.Car, com.example.Bar",
            )
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is Car)
    }

    @Test
    fun testDeserialize_classNotInAllowlist_repeatedRecords_allReturnJsonDataWithSchema() {
        // The allowlist-miss warning is only logged once per class name; verify that suppressing
        // the log on later records does not change what those records deserialize to.
        val deserializer = deserializerWithAllowlist("com.example.SomeOtherClass")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        for (i in 0 until 3) {
            val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

            assertTrue(result is JsonDataWithSchema, "record $i should not resolve to a POJO")
            assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
        }
    }

    @Test
    fun testDeserialize_schemaWithClassName_resolutionDisabled_returnsJsonDataWithSchema() {
        val deserializer = deserializerWithClassNameResolution(false)
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        // Customer opted out of className resolution -> generic JsonDataWithSchema even though
        // the schema carries a className.
        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_classNameInAllowedPackage_returnsSpecificPojo() {
        // A package entry spares consumers from listing every POJO on the topic.
        val deserializer =
            deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.json.*")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is Car)
    }

    @Test
    fun testDeserialize_classNameOutsideAllowedPackage_returnsJsonDataWithSchema() {
        // The allowed package is a sibling of the one the schema names, so it must not match.
        val deserializer = deserializerWithAllowlist("com.amazonaws.services.schemaregistry.other.*")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_manyDistinctDisallowedClassNames_warnStateStaysBounded() {
        // The warning dedup key is the schema's className, which the producer controls. Feeding a
        // stream of distinct disallowed names must not grow the dedup set without bound.
        val deserializer = deserializerWithAllowlist("com.example.SomeOtherClass")

        for (i in 0 until 500) {
            val schema =
                Schema(schemaWithClassName("com.example.Generated$i"), DataFormat.JSON.name, "testJson")

            val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

            assertTrue(result is JsonDataWithSchema, "record $i should not resolve to a POJO")
        }

        assertTrue(
            deserializer.warnedClassNames.size <= MAX_WARNED_CLASS_NAMES,
            "warned class name set grew past the cap: " + deserializer.warnedClassNames.size,
        )
    }

    @Test
    fun testDeserialize_belowWarnCap_doesNotAnnounceSuppression() {
        // Suppression must not kick in for a class count a real consumer could reach.
        val deserializer = deserializerWithAllowlist("com.example.SomeOtherClass")

        for (i in 0 until MAX_WARNED_CLASS_NAMES - 1) {
            val schema =
                Schema(schemaWithClassName("com.example.Generated$i"), DataFormat.JSON.name, "testJson")
            deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)
        }

        assertEquals(MAX_WARNED_CLASS_NAMES - 1, deserializer.warnedClassNames.size)
        assertFalse(
            deserializer.warnCapNoticeEmitted.get(),
            "suppression was announced before the cap was reached",
        )
    }

    @Test
    fun testDeserialize_pastWarnCap_announcesSuppressionOnceAndStopsCollecting() {
        // Past the cap the deserializer stops warning rather than warning per record, and says so
        // once so that the silence is not itself a surprise.
        val deserializer = deserializerWithAllowlist("com.example.SomeOtherClass")

        for (i in 0 until MAX_WARNED_CLASS_NAMES + 50) {
            val schema =
                Schema(schemaWithClassName("com.example.Generated$i"), DataFormat.JSON.name, "testJson")
            deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)
        }

        // Single-threaded, so the cap is exact: names seen after it are not retained at all.
        assertEquals(MAX_WARNED_CLASS_NAMES, deserializer.warnedClassNames.size)
        assertFalse(
            deserializer.warnedClassNames.contains("com.example.Generated$MAX_WARNED_CLASS_NAMES"),
            "a class name seen past the cap was still added to the dedup set",
        )
        assertTrue(
            deserializer.warnCapNoticeEmitted.get(),
            "reaching the cap did not announce that warnings are suppressed",
        )
    }

    @Test
    fun testDeserialize_classNameInSubPackageOfAllowedPackage_returnsJsonDataWithSchema() {
        // The allowed package is the parent of the one the schema names. Allowing a package is not
        // a decision to allow everything beneath it, so this must not resolve.
        val deserializer = deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.*")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD), schema)

        assertTrue(result is JsonDataWithSchema)
        assertEquals(CAR_PAYLOAD, (result as JsonDataWithSchema).payload)
    }

    @Test
    fun testDeserialize_unknownProperty_failsByDefault() {
        val deserializer =
            deserializerWithAllowlist("com.amazonaws.services.schemaregistry.serializers.json.Car")
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD_WITH_UNKNOWN_PROPERTY), schema)
            }

        assertTrue(exception.cause is UnrecognizedPropertyException)
    }

    @Test
    fun testDeserialize_unknownProperty_toleratedWhenTheFeatureIsToggledOff() {
        val configs = HashMap<String, Any>()
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "true"
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] =
            "com.amazonaws.services.schemaregistry.serializers.json.Car"
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name to false)
        val deserializer = JsonDeserializer(GlueSchemaRegistryConfiguration(configs))
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val result = deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD_WITH_UNKNOWN_PROPERTY), schema)

        assertTrue(result is Car)
    }

    @Test
    fun testDeserialize_featureListShapeStillOnlyEnables() {
        val configs = HashMap<String, Any>()
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "true"
        configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] =
            "com.amazonaws.services.schemaregistry.serializers.json.Car"
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            listOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name)
        val deserializer = JsonDeserializer(GlueSchemaRegistryConfiguration(configs))
        val schema = Schema(CAR_SCHEMA, DataFormat.JSON.name, "testJson")

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                deserializer.deserialize(toSerializedBuffer(CAR_PAYLOAD_WITH_UNKNOWN_PROPERTY), schema)
            }

        assertTrue(exception.cause is UnrecognizedPropertyException)
    }

    companion object {
        private const val GEOLOCATION_SCHEMA =
            """{"${'$'}id":"https://example.com/geographical-location.schema.json",""" +
                """"${'$'}schema":"http://json-schema.org/draft-07/schema#","title":"Longitude """ +
                """and Latitude Values","description":"A geographical coordinate.",""" +
                """"required":["latitude","longitude"],"type":"object",""" +
                """"properties":{"latitude":{"type":"number","minimum":-90,""" +
                """"maximum":90},"longitude":{"type":"number","minimum":-180,""" +
                """"maximum":180}},"additionalProperties":false}"""
        private const val GEOLOCATION_PAYLOAD = """{"latitude":48.858093,"longitude":2.294694}"""

        private const val CAR_SCHEMA =
            """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","title":"Simple Car """ +
                """Schema","type":"object","additionalProperties":false,""" +
                """"description":"This is a car","className":"com.amazonaws.services""" +
                """.schemaregistry.serializers.json.Car",""" +
                """"properties":{"make":{"type":"string"},"model":{"type":"string"}}}"""
        private const val CAR_PAYLOAD = """{"make":"Honda","model":"Civic"}"""
        private const val CAR_PAYLOAD_WITH_UNKNOWN_PROPERTY =
            """{"make":"Honda","model":"Civic","colour":"red"}"""

        /**
         * Wraps the raw payload bytes in the Glue Schema Registry header so they can be
         * fed to [JsonDeserializer.deserialize].
         */
        private fun toSerializedBuffer(payload: String): ByteBuffer {
            val data = payload.toByteArray(StandardCharsets.UTF_8)
            val byteBuffer = ByteBuffer.allocate(18 + data.size)
            byteBuffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE)
            byteBuffer.put(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE)
            // Schema version id (128-bit UUID) - value irrelevant for these tests.
            byteBuffer.putLong(0L)
            byteBuffer.putLong(0L)
            byteBuffer.put(data)
            byteBuffer.rewind()
            return byteBuffer
        }

        private fun deserializerWithClassNameResolution(enabled: Boolean): JsonDeserializer {
            val configs = HashMap<String, Any>()
            configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
            configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = enabled.toString()
            return JsonDeserializer(GlueSchemaRegistryConfiguration(configs))
        }

        /**
         * Builds a minimal JSON schema carrying the given `className`, to exercise the
         * deserializer against class names a producer could have put in the schema.
         */
        private fun schemaWithClassName(className: String): String = """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","title":"Simple Car """ +
            """Schema","type":"object","additionalProperties":false,""" +
            """"className":"$className",""" +
            """"properties":{"make":{"type":"string"},"model":{"type":"string"}}}"""

        private fun deserializerWithAllowlist(allowlist: String): JsonDeserializer {
            val configs = HashMap<String, Any>()
            configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
            configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "true"
            configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = allowlist
            return JsonDeserializer(GlueSchemaRegistryConfiguration(configs))
        }
    }
}
