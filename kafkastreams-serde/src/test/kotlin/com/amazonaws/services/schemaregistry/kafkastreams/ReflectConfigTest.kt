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

package com.amazonaws.services.schemaregistry.kafkastreams

import org.apache.kafka.common.serialization.Serde
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class ReflectConfigTest {
    @Test
    fun `every class in the native-image reflect config is a Serde with a no-arg constructor`() {
        val config = javaClass.classLoader.getResource(REFLECT_CONFIG)
        assertNotNull(config, "missing $REFLECT_CONFIG")
        val names = CLASS_NAME.findAll(config!!.readText()).map { it.groupValues[1] }.toList()
        assertEquals(
            setOf(
                GlueSchemaRegistryKafkaStreamsSerde::class.java.name,
                AWSKafkaAvroSerDe::class.java.name,
            ),
            names.toSet(),
        )
        names.forEach { name ->
            val type = Class.forName(name)
            assertEquals(true, Serde::class.java.isAssignableFrom(type), name)
            type.getDeclaredConstructor()
        }
    }

    companion object {
        private val CLASS_NAME = Regex("\"name\"\\s*:\\s*\"([^\"]*\\.[^\"]*)\"")

        private const val REFLECT_CONFIG =
            "META-INF/native-image/com.mobsuccess/schema-registry-kafkastreams-serde/reflect-config.json"
    }
}
