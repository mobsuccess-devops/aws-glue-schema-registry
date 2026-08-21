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

package com.amazonaws.services.schemaregistry.kafkaconnect

import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AWSKafkaAvroConverterConfigTest {
    @Test
    fun testConfigDef_isNoLongerEmpty() {
        val keys = AWSKafkaAvroConverterConfig.configDef().configKeys().keys

        assertTrue(
            keys.containsAll(
                listOf(
                    AWSSchemaRegistryConstants.AWS_REGION,
                    AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                    AWSSchemaRegistryConstants.ASSUME_ROLE_ARN,
                    AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME,
                    AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
                    AvroDataConfig.CONNECT_META_DATA_CONFIG,
                    AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG,
                ),
            ),
        )
    }

    @Test
    fun testConverter_exposesTheConfigDefToConnect() {
        assertEquals(
            AWSKafkaAvroConverterConfig.configDef().configKeys().keys,
            AWSKafkaAvroConverter().config().configKeys().keys,
        )
    }

    @Test
    fun testConfig_acceptsTheDocumentedValues() {
        val config =
            AWSKafkaAvroConverterConfig(
                mapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1",
                    AWSSchemaRegistryConstants.AVRO_RECORD_TYPE to "SPECIFIC_RECORD",
                    AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
                ),
            )

        assertEquals("eu-west-1", config.getString(AWSSchemaRegistryConstants.AWS_REGION))
        assertEquals("SPECIFIC_RECORD", config.getString(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE))
        assertTrue(config.getBoolean(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING))
    }

    @Test
    fun testConfig_rejectsAnUnknownAvroRecordType() {
        assertThrows(ConfigException::class.java) {
            AWSKafkaAvroConverterConfig(mapOf(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE to "MAGIC_RECORD"))
        }
    }

    @Test
    fun testConfig_defaultsTheAssumeRoleSessionName() {
        val config = AWSKafkaAvroConverterConfig(mapOf(AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1"))

        assertEquals(
            AWSKafkaAvroConverterConfig.ASSUME_ROLE_SESSION_NAME_DEFAULT,
            config.getString(AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME),
        )
    }
}
