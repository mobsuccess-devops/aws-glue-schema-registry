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

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class ProtobufSchemaLoaderResourceConfigTest {
    @Test
    fun `every proto the loader reads is declared in the native-image resource config`() {
        assertEquals(protosLoadedFromTheClasspath(), declaredResources())
    }

    @Test
    fun `every declared resource exists on the classpath`() {
        declaredResources().forEach { resource ->
            assertNotNull(
                javaClass.classLoader.getResource(resource),
                "declared in $CONFIG but absent from the classpath: $resource",
            )
        }
    }

    private fun declaredResources(): Set<String> {
        val config = javaClass.classLoader.getResourceAsStream(CONFIG)
        assertNotNull(config, "missing $CONFIG")
        val includes =
            ObjectMapper()
                .readTree(config)
                .path("resources")
                .path("includes")
        return includes
            .map { it.path("pattern").asText().removePrefix("\\Q").removeSuffix("\\E") }
            .toSet()
    }

    private fun protosLoadedFromTheClasspath(): Set<String> {
        fun field(name: String): Any = ProtobufSchemaLoader::class.java
            .getDeclaredField(name)
            .apply { isAccessible = true }
            .get(null)

        @Suppress("UNCHECKED_CAST")
        fun protos(
            path: String,
            names: String,
        ): Set<String> = (field(names) as Set<String>).map { field(path) as String + it }.toSet()

        return protos("GOOGLE_API_PATH", "GOOGLE_API_PROTOS") +
            protos("GOOGLE_WELLKNOWN_PATH", "GOOGLE_WELLKNOWN_PROTOS") +
            protos("WIRE_PATH", "WIRE_PROTOS") +
            (field("METADATA_PATH") as String + field("METADATA_PROTO") as String) +
            (field("DECIMAL_PATH") as String + field("DECIMAL_PROTO") as String)
    }

    companion object {
        private const val CONFIG =
            "META-INF/native-image/com.mobsuccess/schema-registry-serde/resource-config.json"
    }
}
