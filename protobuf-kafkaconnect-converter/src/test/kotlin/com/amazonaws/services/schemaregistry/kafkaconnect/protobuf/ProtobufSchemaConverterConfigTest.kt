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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ProtobufSchemaConverterConfigTest {
    @Test
    fun testConfigDef_isNoLongerEmpty() {
        val keys = ProtobufSchemaConverterConfig.configDef().configKeys().keys

        assertTrue(
            keys.containsAll(
                listOf(
                    AWSSchemaRegistryConstants.AWS_REGION,
                    AWSSchemaRegistryConstants.DATA_FORMAT,
                    AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE,
                ),
            ),
        )
    }

    @Test
    fun testConverter_exposesTheConfigDefToConnect() {
        assertEquals(
            ProtobufSchemaConverterConfig.configDef().configKeys().keys,
            ProtobufSchemaConverter().config().configKeys().keys,
        )
    }

    @Test
    fun testConfig_acceptsTheDocumentedValues() {
        val config =
            ProtobufSchemaConverterConfig(
                mapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1",
                    AWSSchemaRegistryConstants.DATA_FORMAT to "PROTOBUF",
                    AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE to "POJO",
                ),
            )

        assertEquals("PROTOBUF", config.getString(AWSSchemaRegistryConstants.DATA_FORMAT))
        assertEquals("POJO", config.getString(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE))
    }

    @Test
    fun testConfig_rejectsAnUnknownProtobufMessageType() {
        assertThrows(ConfigException::class.java) {
            ProtobufSchemaConverterConfig(mapOf(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE to "POCO"))
        }
    }
}
