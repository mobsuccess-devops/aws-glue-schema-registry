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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import java.net.URI

class GlueSchemaRegistryConfigBuilderTest {
    @Test
    fun testConfig_carriesOnlyTheKeysThatWereSet() {
        val properties = glueSchemaRegistryConfig { region = "eu-west-1" }

        assertEquals(mapOf<String, Any>(AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1"), properties)
    }

    @Test
    fun testConfig_isEmptyWhenNothingIsSet() {
        assertEquals(emptyMap<String, Any>(), glueSchemaRegistryConfig { })
    }

    @Test
    fun testConfig_removesAKeySetBackToNull() {
        val properties =
            glueSchemaRegistryConfig {
                registryName = "mine"
                registryName = null
            }

        assertFalse(properties.containsKey(AWSSchemaRegistryConstants.REGISTRY_NAME))
    }

    @Test
    fun testConfig_readsBackEveryTypedProperty() {
        val builder = GlueSchemaRegistryConfigBuilder()
        builder.apply {
            region = "eu-west-1"
            endpoint = "https://glue.localhost"
            proxyUrl = URI.create("http://proxy.example.com:8080")
            registryName = "mine"
            schemaName = "User"
            schemaNamingStrategyClass = "com.example.Naming"
            autoRegistration = true
            compatibility = Compatibility.FULL
            description = "the user schema"
            compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB
            dataFormat = DataFormat.AVRO
            avroRecordType = AvroRecordType.SPECIFIC_RECORD
            protobufMessageType = ProtobufMessageType.POJO
            secondaryDeserializer = "com.example.Legacy"
            userAgentApp = "my-app"
            cacheSize = 500
            cacheTimeToLiveMillis = 60_000L
            jsonClassNameResolutionEnabled = true
            jsonSchemaNullableEnabled = true
            jsonSchemaCompatibilityCheckEnabled = true
        }

        assertEquals("eu-west-1", builder.region)
        assertEquals("https://glue.localhost", builder.endpoint)
        assertEquals(URI.create("http://proxy.example.com:8080"), builder.proxyUrl)
        assertEquals("mine", builder.registryName)
        assertEquals("User", builder.schemaName)
        assertEquals("com.example.Naming", builder.schemaNamingStrategyClass)
        assertEquals(true, builder.autoRegistration)
        assertEquals(Compatibility.FULL, builder.compatibility)
        assertEquals("the user schema", builder.description)
        assertEquals(AWSSchemaRegistryConstants.COMPRESSION.ZLIB, builder.compression)
        assertEquals(DataFormat.AVRO, builder.dataFormat)
        assertEquals(AvroRecordType.SPECIFIC_RECORD, builder.avroRecordType)
        assertEquals(ProtobufMessageType.POJO, builder.protobufMessageType)
        assertEquals("com.example.Legacy", builder.secondaryDeserializer)
        assertEquals("my-app", builder.userAgentApp)
        assertEquals(500, builder.cacheSize)
        assertEquals(60_000L, builder.cacheTimeToLiveMillis)
        assertEquals(true, builder.jsonClassNameResolutionEnabled)
        assertEquals(true, builder.jsonSchemaNullableEnabled)
        assertEquals(true, builder.jsonSchemaCompatibilityCheckEnabled)
    }

    @Test
    fun testConfig_isReadByTheRegistryConfigurationItConfigures() {
        val configuration =
            glueSchemaRegistryConfiguration {
                region = "eu-west-1"
                registryName = "mine"
                schemaName = "User"
                autoRegistration = true
                compatibility = Compatibility.FULL
                compression = AWSSchemaRegistryConstants.COMPRESSION.ZLIB
                avroRecordType = AvroRecordType.SPECIFIC_RECORD
                protobufMessageType = ProtobufMessageType.POJO
                cacheSize = 500
                cacheTimeToLiveMillis = 60_000L
                jsonSchemaNullableEnabled = true
                jsonSchemaCompatibilityCheckEnabled = true
            }

        assertEquals("eu-west-1", configuration.region)
        assertEquals("mine", configuration.registryName)
        assertTrue(configuration.isSchemaAutoRegistrationEnabled)
        assertEquals(Compatibility.FULL, configuration.compatibilitySetting)
        assertEquals(AWSSchemaRegistryConstants.COMPRESSION.ZLIB, configuration.compressionType)
        assertEquals(AvroRecordType.SPECIFIC_RECORD, configuration.avroRecordType)
        assertEquals(ProtobufMessageType.POJO, configuration.protobufMessageType)
        assertEquals(500, configuration.cacheSize)
        assertEquals(60_000L, configuration.timeToLiveMillis)
        assertTrue(configuration.isJsonSchemaNullableEnabled)
        assertTrue(configuration.isJsonSchemaCompatibilityCheckEnabled)
    }

    @Test
    fun testConfig_leavesTheLibraryDefaultsAlone() {
        val fromDsl = glueSchemaRegistryConfiguration { region = "eu-west-1" }
        val fromMap = GlueSchemaRegistryConfiguration(mapOf(AWSSchemaRegistryConstants.AWS_REGION to "eu-west-1"))

        assertEquals(fromMap, fromDsl)
    }

    @Test
    fun testTags_areHandedOverAsAHashMap() {
        val configuration =
            glueSchemaRegistryConfiguration {
                region = "eu-west-1"
                tags(mapOf("owner" to "data-platform"))
                metadata(mapOf("team" to "ingest"))
            }

        assertEquals(mapOf("owner" to "data-platform"), configuration.tags)
        assertEquals(mapOf("team" to "ingest"), configuration.metadata)
    }

    @Test
    fun testJacksonFeatures_areHandedOverAsEnumNames() {
        val configuration =
            glueSchemaRegistryConfiguration {
                region = "eu-west-1"
                jacksonSerializationFeatures(SerializationFeature.INDENT_OUTPUT)
                jacksonDeserializationFeatures(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            }

        assertEquals(listOf(SerializationFeature.INDENT_OUTPUT), configuration.jacksonSerializationFeatures)
        assertEquals(
            listOf(DeserializationFeature.FAIL_ON_TRAILING_TOKENS),
            configuration.jacksonDeserializationFeatures,
        )
    }

    @Test
    fun testAllowlist_acceptsNamesAndClasses() {
        val fromNames =
            glueSchemaRegistryConfiguration {
                region = "eu-west-1"
                jsonClassNameAllowlist(GlueSchemaRegistryConfiguration::class.java.name)
            }
        val fromClasses =
            glueSchemaRegistryConfiguration {
                region = "eu-west-1"
                jsonClassNameAllowlist(GlueSchemaRegistryConfiguration::class.java)
            }

        assertEquals(fromNames.jsonClassNameAllowlist, fromClasses.jsonClassNameAllowlist)
        assertTrue(fromClasses.isClassNameAllowed(GlueSchemaRegistryConfiguration::class.java.name))
    }

    @Test
    fun testProperty_setsAKeyWithNoTypedAccessor() {
        val properties =
            glueSchemaRegistryConfig {
                property(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN, "arn:aws:iam::1:role/r")
            }

        assertEquals("arn:aws:iam::1:role/r", properties[AWSSchemaRegistryConstants.ASSUME_ROLE_ARN])
    }

    @Test
    fun testProperty_readsBackNullForAnUnsetKey() {
        assertNull(GlueSchemaRegistryConfigBuilder().region)
    }
}
