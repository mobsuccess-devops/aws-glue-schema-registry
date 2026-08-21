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

package com.mobsuccess.schemaregistry.kotlin

import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.glue.model.DataFormat

class GlueSchemaRegistryFactoriesTest {
    @Test
    fun testSerializer_isConfiguredFromTheBlock() {
        val serializer =
            glueSchemaRegistrySerializer {
                region = REGION
                dataFormat = DataFormat.AVRO
                autoRegistration = true
            }

        assertEquals(DataFormat.AVRO.toString(), serializer.dataFormat)
        assertEquals(UserAgents.KAFKA, serializer.userAgentApp)
        assertNotNull(serializer.glueSchemaRegistrySerializationFacade)
    }

    @Test
    fun testSerializer_configuresForTheKeyWhenAsked() {
        val serializer =
            glueSchemaRegistrySerializer(isKey = true) {
                region = REGION
                dataFormat = DataFormat.AVRO
            }

        assertTrue(serializer.isKey)
    }

    @Test
    fun testDeserializer_isConfiguredFromTheBlock() {
        val deserializer =
            glueSchemaRegistryDeserializer {
                region = REGION
            }

        assertNotNull(deserializer.glueSchemaRegistryDeserializationFacade)
    }

    @Test
    fun testSerde_isTypedAndConfiguredFromTheBlock() {
        val serde =
            glueSchemaRegistrySerde<String> {
                region = REGION
                dataFormat = DataFormat.AVRO
            }

        assertNotNull(serde.serializer())
        assertNotNull(serde.deserializer())
    }

    @Test
    fun testSerde_acceptsATypeKnownAtRuntime() {
        val serde =
            glueSchemaRegistrySerde(String::class.java) {
                region = REGION
                dataFormat = DataFormat.AVRO
            }

        assertNotNull(serde.deserializer())
    }

    private companion object {
        private const val REGION = "us-west-2"
    }
}
