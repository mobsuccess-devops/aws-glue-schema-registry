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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class JsonSchemaConverterConfigTest {
    @Test
    fun testConfigDef_isNoLongerEmpty() {
        val keys = JsonSchemaConverterConfig.configDef().configKeys().keys

        assertTrue(
            keys.containsAll(
                listOf(
                    AWSSchemaRegistryConstants.AWS_REGION,
                    AWSSchemaRegistryConstants.DATA_FORMAT,
                    AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED,
                    AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST,
                    AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES,
                    JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG,
                    JsonSchemaDataConfig.CONNECT_META_DATA_CONFIG,
                ),
            ),
        )
    }

    @Test
    fun testConverter_exposesTheConfigDefToConnect() {
        assertEquals(
            JsonSchemaConverterConfig.configDef().configKeys().keys,
            JsonSchemaConverter().config().configKeys().keys,
        )
    }

    @Test
    fun testConfig_readsTheAllowlistFromACommaSeparatedString() {
        val config =
            JsonSchemaConverterConfig(
                mapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1",
                    AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST to "com.example.A,com.example.pojos.*",
                ),
            )

        assertEquals(
            listOf("com.example.A", "com.example.pojos.*"),
            config.getList(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST),
        )
    }

    @Test
    fun testConfig_rejectsAnUnknownDataFormat() {
        assertThrows(ConfigException::class.java) {
            JsonSchemaConverterConfig(mapOf(AWSSchemaRegistryConstants.DATA_FORMAT to "YAML"))
        }
    }
}
