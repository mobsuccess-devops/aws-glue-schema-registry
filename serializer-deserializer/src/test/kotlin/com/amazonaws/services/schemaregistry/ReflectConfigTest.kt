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

package com.amazonaws.services.schemaregistry

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.DescriptorProtos
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReflectConfigTest {
    @Test
    fun `the reflect config registers exactly the classes a configuration names as a string`() {
        assertEquals(
            setOf(
                GlueSchemaRegistryKafkaSerializer::class.java.name,
                AWSKafkaAvroSerializer::class.java.name,
                GlueSchemaRegistryKafkaDeserializer::class.java.name,
                AWSKafkaAvroDeserializer::class.java.name,
                DescriptorProtos::class.java.name,
            ),
            declaredReflection().keys,
        )
    }

    @Test
    fun `every entry point is a Kafka serializer or deserializer built from a no-arg constructor`() {
        entryPoints.forEach { entryPoint ->
            assertTrue(
                Serializer::class.java.isAssignableFrom(entryPoint) ||
                    Deserializer::class.java.isAssignableFrom(entryPoint),
                "${entryPoint.name} is registered as an entry point but is neither a Serializer nor a Deserializer",
            )
            entryPoint.getDeclaredConstructor()
        }
    }

    @Test
    fun `every constructor the reflect config declares exists`() {
        declaredReflection().forEach { (name, constructors) ->
            val type = Class.forName(name)
            constructors.forEach { parameterTypes ->
                type.getDeclaredConstructor(*parameterTypes.map { Class.forName(it) }.toTypedArray())
            }
        }
    }

    private fun declaredReflection(): Map<String, List<List<String>>> {
        val config = javaClass.classLoader.getResourceAsStream(REFLECT_CONFIG)
        assertNotNull(config, "missing $REFLECT_CONFIG")
        return ObjectMapper()
            .readTree(config)
            .associate { entry ->
                entry.path("name").asText() to
                    entry
                        .path("methods")
                        .filter { it.path("name").asText() == "<init>" }
                        .map { method -> method.path("parameterTypes").map { it.asText() } }
            }
    }

    companion object {
        private const val REFLECT_CONFIG =
            "META-INF/native-image/com.mobsuccess/schema-registry-serde/reflect-config.json"

        private val entryPoints =
            listOf(
                GlueSchemaRegistryKafkaSerializer::class.java,
                AWSKafkaAvroSerializer::class.java,
                GlueSchemaRegistryKafkaDeserializer::class.java,
                AWSKafkaAvroDeserializer::class.java,
            )
    }
}
