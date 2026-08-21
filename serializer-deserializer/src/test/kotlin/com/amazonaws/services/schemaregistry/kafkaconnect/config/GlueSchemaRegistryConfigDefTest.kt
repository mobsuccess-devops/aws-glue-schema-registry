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

package com.amazonaws.services.schemaregistry.kafkaconnect.config

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.stream.Stream

class GlueSchemaRegistryConfigDefTest {
    @Test
    fun testBaseConfigDef_declaresTheRegistryKeys() {
        val keys = GlueSchemaRegistryConfigDef.baseConfigDef().configKeys().keys

        assertTrue(
            keys.containsAll(
                listOf(
                    AWSSchemaRegistryConstants.AWS_REGION,
                    AWSSchemaRegistryConstants.AWS_ENDPOINT,
                    AWSSchemaRegistryConstants.PROXY_URL,
                    AWSSchemaRegistryConstants.REGISTRY_NAME,
                    AWSSchemaRegistryConstants.SCHEMA_NAME,
                    AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
                    AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                    AWSSchemaRegistryConstants.COMPATIBILITY_SETTING,
                    AWSSchemaRegistryConstants.DESCRIPTION,
                    AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                    AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER,
                    AWSSchemaRegistryConstants.CACHE_SIZE,
                    AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS,
                ),
            ),
        )
    }

    @Test
    fun testBaseConfigDef_documentsAndGroupsEveryKey() {
        GlueSchemaRegistryConfigDef.baseConfigDef().configKeys().values.forEach { key ->
            assertTrue(key.documentation.isNotBlank(), "${key.name} has no documentation")
            assertNotNull(key.group, "${key.name} has no group")
            assertNotNull(key.displayName, "${key.name} has no display name")
        }
    }

    @Test
    fun testBaseConfigDef_defaultsMatchTheRegistryConfiguration() {
        val reference = GlueSchemaRegistryConfiguration(mapOf(AWSSchemaRegistryConstants.AWS_REGION to "us-east-1"))
        val parsed = AbstractConfig(GlueSchemaRegistryConfigDef.baseConfigDef(), emptyMap<String, Any>())

        assertEquals(reference.registryName, parsed.getString(AWSSchemaRegistryConstants.REGISTRY_NAME))
        assertEquals(reference.cacheSize, parsed.getInt(AWSSchemaRegistryConstants.CACHE_SIZE))
        assertEquals(
            reference.timeToLiveMillis,
            parsed.getLong(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS),
        )
        assertEquals(
            reference.compressionType.name,
            parsed.getString(AWSSchemaRegistryConstants.COMPRESSION_TYPE),
        )
        assertEquals(
            reference.compatibilitySetting.toString(),
            parsed.getString(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING),
        )
        assertEquals(
            reference.isSchemaAutoRegistrationEnabled,
            parsed.getBoolean(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING),
        )
        assertNull(parsed.getString(AWSSchemaRegistryConstants.AWS_REGION))
    }

    @ParameterizedTest
    @MethodSource("acceptedValues")
    fun testValidators_acceptTheValuesTheRegistryConfigurationAccepts(
        key: String,
        value: String,
    ) {
        val configDef = fullConfigDef()

        assertEquals(value, AbstractConfig(configDef, mapOf(key to value)).getString(key))
    }

    @ParameterizedTest
    @MethodSource("rejectedValues")
    fun testValidators_rejectUnknownValues(
        key: String,
        value: String,
    ) {
        val configDef = fullConfigDef()

        assertThrows(ConfigException::class.java) { AbstractConfig(configDef, mapOf(key to value)) }
    }

    @Test
    fun testCoerce_rendersANonStringValueOfAStringKey() {
        val configDef = fullConfigDef()
        val coerced =
            GlueSchemaRegistryConfigDef.coerce(
                configDef,
                mapOf(AWSSchemaRegistryConstants.COMPRESSION_TYPE to AWSSchemaRegistryConstants.COMPRESSION.ZLIB),
            )

        assertEquals("ZLIB", coerced[AWSSchemaRegistryConstants.COMPRESSION_TYPE])
    }

    @Test
    fun testCoerce_rendersAClassAsItsBinaryName() {
        val configDef = fullConfigDef()
        val coerced =
            GlueSchemaRegistryConfigDef.coerce(
                configDef,
                mapOf(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER to GlueSchemaRegistryConfiguration::class.java),
            )

        assertEquals(
            GlueSchemaRegistryConfiguration::class.java.name,
            coerced[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER],
        )
    }

    @Test
    fun testCoerce_leavesNonStringKeysAndUnknownKeysUntouched() {
        val configDef = fullConfigDef()
        val tags = mapOf("owner" to "data-platform")
        val coerced =
            GlueSchemaRegistryConfigDef.coerce(
                configDef,
                mapOf(
                    AWSSchemaRegistryConstants.CACHE_SIZE to 42,
                    AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING to true,
                    AWSSchemaRegistryConstants.TAGS to tags,
                    AWSSchemaRegistryConstants.AWS_REGION to null,
                ),
            )

        assertEquals(42, coerced[AWSSchemaRegistryConstants.CACHE_SIZE])
        assertEquals(true, coerced[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING])
        assertEquals(tags, coerced[AWSSchemaRegistryConstants.TAGS])
        assertNull(coerced[AWSSchemaRegistryConstants.AWS_REGION])
    }

    @Test
    fun testConfigDef_leavesTheMapValuedKeysUndeclared() {
        val keys = fullConfigDef().configKeys().keys

        assertFalse(keys.contains(AWSSchemaRegistryConstants.TAGS))
        assertFalse(keys.contains(AWSSchemaRegistryConstants.METADATA))
    }

    @Test
    fun testJacksonFeatures_rejectAnUnknownFeatureName() {
        val configDef = fullConfigDef()

        assertThrows(ConfigException::class.java) {
            AbstractConfig(
                configDef,
                mapOf(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES to "NOT_A_FEATURE"),
            )
        }
        assertThrows(ConfigException::class.java) {
            AbstractConfig(
                configDef,
                mapOf(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES to listOf("NOT_A_FEATURE")),
            )
        }
    }

    @Test
    fun testJacksonFeatures_acceptAKnownFeatureName() {
        val parsed =
            AbstractConfig(
                fullConfigDef(),
                mapOf(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES to "INDENT_OUTPUT,WRAP_ROOT_VALUE"),
            )

        assertEquals(
            listOf("INDENT_OUTPUT", "WRAP_ROOT_VALUE"),
            parsed.getList(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES),
        )
    }

    @Test
    fun testAllowlist_rejectsABareWildcard() {
        val configDef = fullConfigDef()

        assertThrows(ConfigException::class.java) {
            AbstractConfig(configDef, mapOf(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST to "*"))
        }
        assertThrows(ConfigException::class.java) {
            AbstractConfig(configDef, mapOf(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST to ".*"))
        }
    }

    @Test
    fun testAllowlist_acceptsAScopedPackage() {
        val parsed =
            AbstractConfig(
                fullConfigDef(),
                mapOf(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST to "com.example.pojos.*"),
            )

        assertEquals(
            listOf("com.example.pojos.*"),
            parsed.getList(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST),
        )
    }

    @Test
    fun testDefineDataFormat_acceptsOnlyTheConvertersOwnFormat() {
        val protobufConfigDef =
            GlueSchemaRegistryConfigDef.defineDataFormat(
                GlueSchemaRegistryConfigDef.baseConfigDef(),
                DataFormat.PROTOBUF,
            )

        assertEquals(
            "PROTOBUF",
            AbstractConfig(protobufConfigDef, emptyMap<String, Any>())
                .getString(AWSSchemaRegistryConstants.DATA_FORMAT),
        )
        assertThrows(ConfigException::class.java) {
            AbstractConfig(protobufConfigDef, mapOf(AWSSchemaRegistryConstants.DATA_FORMAT to "AVRO"))
        }
    }

    @Test
    fun testCoerce_splitsACommaSeparatedValueOfAListKey() {
        val coerced =
            GlueSchemaRegistryConfigDef.coerce(
                fullConfigDef(),
                mapOf(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES to " INDENT_OUTPUT , ,WRAP_ROOT_VALUE"),
            )

        assertEquals(
            listOf("INDENT_OUTPUT", "WRAP_ROOT_VALUE"),
            coerced[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES],
        )
    }

    @Test
    fun testCoerce_returnsTheSameMapWhenNothingNeedsRendering() {
        val props = mapOf(AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1")

        assertSame(props, GlueSchemaRegistryConfigDef.coerce(fullConfigDef(), props))
    }

    @Test
    fun testConfigDef_leavesTheUserAgentUndeclared() {
        assertFalse(fullConfigDef().configKeys().containsKey(AWSSchemaRegistryConstants.USER_AGENT_APP))
    }

    private fun fullConfigDef(): ConfigDef {
        val configDef = GlueSchemaRegistryConfigDef.baseConfigDef()
        GlueSchemaRegistryConfigDef.defineDataFormat(configDef, DataFormat.JSON)
        GlueSchemaRegistryConfigDef.defineAvro(configDef)
        GlueSchemaRegistryConfigDef.defineProtobuf(configDef)
        GlueSchemaRegistryConfigDef.defineJson(configDef)
        return configDef
    }

    companion object {
        @JvmStatic
        fun acceptedValues(): Stream<Array<String>> = Stream.of(
            arrayOf(AWSSchemaRegistryConstants.COMPRESSION_TYPE, "ZLIB"),
            arrayOf(AWSSchemaRegistryConstants.COMPRESSION_TYPE, "zlib"),
            arrayOf(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "FULL"),
            arrayOf(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "full"),
            arrayOf(AWSSchemaRegistryConstants.DATA_FORMAT, "JSON"),
            arrayOf(AWSSchemaRegistryConstants.DATA_FORMAT, "json"),
            arrayOf(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "SPECIFIC_RECORD"),
            arrayOf(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "DYNAMIC_MESSAGE"),
        )

        @JvmStatic
        fun rejectedValues(): Stream<Array<String>> = Stream.of(
            arrayOf(AWSSchemaRegistryConstants.COMPRESSION_TYPE, "GZIP"),
            arrayOf(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "SOMETIMES"),
            arrayOf(AWSSchemaRegistryConstants.DATA_FORMAT, "YAML"),
            arrayOf(AWSSchemaRegistryConstants.DATA_FORMAT, "PROTOBUF"),
            arrayOf(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "specific_record"),
            arrayOf(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, "dynamic_message"),
        )
    }
}
