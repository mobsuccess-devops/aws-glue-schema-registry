/*
 * Copyright 2026 Mobsuccess.
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

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.DefaultObjectMapperFactory
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.ObjectMapperFactory
import com.amazonaws.services.schemaregistry.deserializers.json.JsonDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate

/**
 * Tests the two properties that let an application decide how the JSON serializer and
 * deserializer build their Jackson mappers.
 */
class JsonObjectMapperCustomizationTest {
    @Test
    fun testSerialize_javaTimePojoWithoutTheModule_fails() {
        val serializer = JsonSerializer(configuration(emptyMap()))

        assertThrows(AWSSchemaRegistryException::class.java) { serializer.serialize(RESERVATION) }
    }

    @Test
    fun testSerialize_javaTimePojoWithTheModule_writesTheIsoRepresentation() {
        val serializer = JsonSerializer(configuration(javaTimeModuleConfigs()))

        val payload = String(serializer.serialize(RESERVATION), StandardCharsets.UTF_8)

        assertTrue(payload.contains(""""checkIn":"2026-03-14""""), payload)
        assertTrue(payload.contains(""""bookedAt":"2026-03-01T09:30:00Z""""), payload)
    }

    @Test
    fun testGetSchemaDefinition_javaTimePojoWithTheModule_describesTheValuesAsStrings() {
        val serializer = JsonSerializer(configuration(javaTimeModuleConfigs()))

        val schemaDefinition = serializer.getSchemaDefinition(RESERVATION)

        assertTrue(schemaDefinition.contains(""""checkIn":{"type":"string""""), schemaDefinition)
        assertTrue(schemaDefinition.contains(""""bookedAt":{"type":"string""""), schemaDefinition)
    }

    /**
     * Tests the round trip the java.time issue asks for: a POJO carrying `java.time` values is
     * serialized against a schema generated from it, and read back into an equal POJO.
     */
    @Test
    fun testRoundTrip_javaTimePojoWithTheModule_readsBackAnEqualPojo() {
        val configs = javaTimeModuleConfigs() + classNameResolutionConfigs()
        val serializer = JsonSerializer(configuration(configs))
        val deserializer = JsonDeserializer(configuration(configs))

        val serializedBytes = serializer.serialize(RESERVATION)
        val schema = Schema(serializer.getSchemaDefinition(RESERVATION), DataFormat.JSON.name, "reservation")

        val result = deserializer.deserialize(toSerializedBuffer(serializedBytes), schema)

        assertEquals(RESERVATION, result)
    }

    @Test
    fun testDeserialize_javaTimePojoWithoutTheModule_fails() {
        val serializer = JsonSerializer(configuration(javaTimeModuleConfigs() + classNameResolutionConfigs()))
        val deserializer = JsonDeserializer(configuration(classNameResolutionConfigs()))
        val serializedBytes = serializer.serialize(RESERVATION)
        val schema = Schema(serializer.getSchemaDefinition(RESERVATION), DataFormat.JSON.name, "reservation")

        assertThrows(AWSSchemaRegistryException::class.java) {
            deserializer.deserialize(toSerializedBuffer(serializedBytes), schema)
        }
    }

    @Test
    fun testSerialize_customFactory_buildsTheSerializerMapper() {
        val configs =
            mapOf(AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY to IndentingObjectMapperFactory::class.java.name)
        val serializer = JsonSerializer(configuration(configs))

        val payload = String(serializer.serialize(UNSORTED_RECORD), StandardCharsets.UTF_8)

        assertTrue(payload.contains("\n"), payload)
    }

    /**
     * Tests the hook the feature lists cannot reach: a `MapperFeature` is neither a
     * `SerializationFeature` nor a `DeserializationFeature`, so the two feature properties have
     * no way of setting one.
     */
    @Test
    fun testDeserialize_customFactory_appliesWhatTheFeatureListsCannot() {
        val configs =
            mapOf(AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY to CaseInsensitiveObjectMapperFactory::class.java.name) +
                classNameResolutionConfigs()
        val deserializer = JsonDeserializer(configuration(configs))
        val schema = Schema(FRUIT_SCHEMA, DataFormat.JSON.name, "fruit")

        val result =
            deserializer.deserialize(
                toSerializedBuffer(SHOUTED_FRUIT_PAYLOAD.toByteArray(StandardCharsets.UTF_8)),
                schema,
            )

        assertEquals(Fruit().apply { name = "apple" }, result)
    }

    @Test
    fun testDeserialize_withoutTheCustomFactory_theSamePayloadIsRefused() {
        val deserializer = JsonDeserializer(configuration(classNameResolutionConfigs()))
        val schema = Schema(FRUIT_SCHEMA, DataFormat.JSON.name, "fruit")

        assertThrows(AWSSchemaRegistryException::class.java) {
            deserializer.deserialize(
                toSerializedBuffer(SHOUTED_FRUIT_PAYLOAD.toByteArray(StandardCharsets.UTF_8)),
                schema,
            )
        }
    }

    @Test
    fun testSerialize_defaultFactoryNamedExplicitly_isByteIdenticalToNamingNothing() {
        val plain = JsonSerializer(configuration(emptyMap()))
        val explicit =
            JsonSerializer(
                configuration(
                    mapOf(
                        AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY to DefaultObjectMapperFactory::class.java.name,
                    ),
                ),
            )

        assertArrayEquals(plain.serialize(UNSORTED_RECORD), explicit.serialize(UNSORTED_RECORD))
        assertEquals(plain.getSchemaDefinition(CAR), explicit.getSchemaDefinition(CAR))
    }

    class CaseInsensitiveObjectMapperFactory : ObjectMapperFactory {
        override fun newObjectMapper(): ObjectMapper = JsonMapper
            .builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
            .build()
    }

    class IndentingObjectMapperFactory : ObjectMapperFactory {
        override fun newObjectMapper(): ObjectMapper = DefaultObjectMapperFactory()
            .newObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
    }

    /** A POJO whose properties Jackson reads and writes on its own, with no annotation. */
    class Fruit {
        var name: String? = null

        override fun equals(other: Any?): Boolean = other is Fruit && name == other.name

        override fun hashCode(): Int = name?.hashCode() ?: 0

        override fun toString(): String = "Fruit(name=$name)"
    }

    companion object {
        private val RESERVATION =
            Reservation.of("R-1", LocalDate.of(2026, 3, 14), Instant.parse("2026-03-01T09:30:00Z"))

        private val CAR = Car.builder().make("Honda").model("Civic").build()

        private const val UNSORTED_SCHEMA =
            """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","type":"object",""" +
                """"properties":{"zebra":{"type":"string"},"apple":{"type":"string"}}}"""
        private const val UNSORTED_PAYLOAD = """{"zebra":"z","apple":"a"}"""

        private const val FRUIT_SCHEMA =
            """{"${'$'}schema":"http://json-schema.org/draft-04/schema#","type":"object",""" +
                """"className":"com.amazonaws.services.schemaregistry.serializers.json.""" +
                """JsonObjectMapperCustomizationTest${'$'}Fruit",""" +
                """"properties":{"NAME":{"type":"string"}}}"""
        private const val SHOUTED_FRUIT_PAYLOAD = """{"NAME":"apple"}"""

        private val UNSORTED_RECORD =
            JsonDataWithSchema.builder(UNSORTED_SCHEMA, UNSORTED_PAYLOAD).build()

        private fun configuration(extra: Map<String, Any>): GlueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(
            HashMap<String, Any>(extra).apply {
                put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2")
            },
        )

        private fun javaTimeModuleConfigs(): Map<String, Any> = mapOf(
            AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE to JavaTimeModule::class.java.name,
            AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES to
                mapOf(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS.name to false),
        )

        private fun classNameResolutionConfigs(): Map<String, Any> = mapOf(
            AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED to "true",
            AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST to
                "com.amazonaws.services.schemaregistry.serializers.json.*",
        )

        /**
         * Wraps payload bytes in the Glue Schema Registry header, as the deserializer expects.
         */
        private fun toSerializedBuffer(data: ByteArray): ByteBuffer {
            val byteBuffer = ByteBuffer.allocate(18 + data.size)
            byteBuffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE)
            byteBuffer.put(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE)
            byteBuffer.putLong(0L)
            byteBuffer.putLong(0L)
            byteBuffer.put(data)
            byteBuffer.rewind()
            return byteBuffer
        }
    }
}
