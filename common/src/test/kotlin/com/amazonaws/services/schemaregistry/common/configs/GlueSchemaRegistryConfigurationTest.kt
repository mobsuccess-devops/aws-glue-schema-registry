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

package com.amazonaws.services.schemaregistry.common.configs

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import software.amazon.awssdk.services.glue.model.Compatibility
import java.math.BigDecimal
import java.net.URI
import java.util.Properties

/**
 * Unit tests for testing configuration elements.
 */
class GlueSchemaRegistryConfigurationTest {
    private val configs = HashMap<String, Any>()

    private val userReaderSchema =
        """{"type":"record","name":"User","namespace":"test","fields":[{"name":"name","type":"string"}]}"""

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    fun setup() {
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
    }

    /**
     * Tears down test data after each test is run.
     */
    @AfterEach
    fun tearDown() {
        configs.clear()
    }

    /**
     * Tests building configuration and checking for values.
     */
    @Test
    fun testBuildConfig_fromMap_succeeds() {
        val configs = HashMap<String, Any>()
        val metadata = HashMap<String, String>()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = "default-topic"

        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test/"
        configs[AWSSchemaRegistryConstants.CACHE_SIZE] = "1000"
        configs[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] = "100"
        configs[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()
        configs[AWSSchemaRegistryConstants.METADATA] = metadata

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(
            glueSchemaRegistryConfiguration.compressionType.name,
            AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name,
        )
        assertEquals("US-West-1", glueSchemaRegistryConfiguration.region)
        assertEquals("https://test/", glueSchemaRegistryConfiguration.endPoint)
        assertEquals(1000, glueSchemaRegistryConfiguration.cacheSize)
        assertEquals(100, glueSchemaRegistryConfiguration.timeToLiveMillis)
        assertEquals(AvroRecordType.GENERIC_RECORD, glueSchemaRegistryConfiguration.avroRecordType)
        assertEquals(metadata, glueSchemaRegistryConfiguration.metadata)
    }

    /**
     * Tests configuration for region value
     */
    @Test
    fun testBuildConfig_noRegionConfigsSupplied_throwsException() {
        val configWithoutRegion = HashMap<String, Any>()
        configWithoutRegion[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test/"
        System.setProperty("aws.profile", "")

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                GlueSchemaRegistryConfiguration(configWithoutRegion)
            }

        assertEquals("Region is not defined in the properties", exception.message)
    }

    /**
     * Tests configuration for region value via default AWS region provider chain
     */
    @Test
    fun testBuildConfig_regionConfigsSuppliedUsingAwsProvider_thenUseDefaultAwsRegionProviderChain() {
        val configWithoutRegion = HashMap<String, Any>()
        configWithoutRegion[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test/"
        System.setProperty("aws.region", "us-west-2")

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configWithoutRegion)

        assertEquals("us-west-2", glueSchemaRegistryConfiguration.region)

        System.clearProperty("aws.region")
    }

    /**
     * Tests configuration for region value
     */
    @Test
    fun testBuildConfig_withRegionConfig_Instantiates() {
        assertDoesNotThrow { GlueSchemaRegistryConfiguration("us-west-1") }
    }

    /**
     * Tests building configuration and checking for values.
     */
    @Test
    fun testBuildConfig_fromProperties_succeeds() {
        val props = createTestProperties()

        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertEquals("US-West-1", glueSchemaRegistryConfiguration.region)
        assertEquals(1000, glueSchemaRegistryConfiguration.cacheSize)
        assertEquals(100, glueSchemaRegistryConfiguration.timeToLiveMillis)
        assertEquals(AvroRecordType.GENERIC_RECORD, glueSchemaRegistryConfiguration.avroRecordType)
    }

    /**
     * Helper method to setup base configuration property elements.
     *
     * @return Properties instance
     */
    private fun createTestProperties(): Properties {
        val props = Properties()
        props[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"
        props[AWSSchemaRegistryConstants.CACHE_SIZE] = "1000"
        props[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] = "100"
        props[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()
        return props
    }

    /**
     * Tests invalid cacheTTL value.
     */
    @Test
    fun testBuildConfig_cacheTTLAsString_throwsException() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] = "Random String"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertEquals("Time to live cache property is not a valid time : Random String", exception.message)
    }

    /**
     * Tests invalid cacheSize value.
     */
    @Test
    fun testBuildConfig_cacheSizeAsString_throwsException() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.CACHE_SIZE] = "Random String"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertEquals("Cache size property is not a valid size : Random String", exception.message)
    }

    /**
     * Tests default values are used if not passed
     */
    @Test
    fun testBuildConfig_valuesNotPassed_usesDefault() {
        val props = Properties()
        props[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"

        val serDeConfigs = GlueSchemaRegistryConfiguration(props)
        assertNotNull(serDeConfigs.cacheSize)
        assertNotNull(serDeConfigs.timeToLiveMillis)
        assertNotNull(serDeConfigs.compressionType == AWSSchemaRegistryConstants.COMPRESSION.NONE)
        assertNotNull(serDeConfigs.compatibilitySetting == Compatibility.NONE)
        assertEquals(AvroRecordType.GENERIC_RECORD, serDeConfigs.avroRecordType)
    }

    /**
     * Tests compatibility value
     */
    @ParameterizedTest
    @EnumSource(
        value = Compatibility::class,
        names = ["UNKNOWN_TO_SDK_VERSION"],
        mode = EnumSource.Mode.EXCLUDE,
    )
    fun testBuildConfig_validCompatibilitySetting_succeeds(compatibility: Compatibility) {
        val props = Properties()
        props[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"
        props[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = compatibility.name

        var serDeConfigs = GlueSchemaRegistryConfiguration(props)

        assertNotNull(serDeConfigs.compatibilitySetting)
        assertEquals(compatibility.name, serDeConfigs.compatibilitySetting!!.name)

        props[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = compatibility.name.lowercase()
        serDeConfigs = GlueSchemaRegistryConfiguration(props)

        assertNotNull(serDeConfigs.compatibilitySetting)
        assertEquals(compatibility.name, serDeConfigs.compatibilitySetting!!.name)
    }

    /**
     * Tests invalid compatibility value
     */
    @Test
    fun testBuildConfig_invalidCompatibilitySetting_throwsException() {
        val props = Properties()
        props[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"
        // invalid compatibility type
        props[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] = "backwards_full"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }
        assertTrue(exception.message!!.contains("Invalid compatibility setting : backwards_full"))
    }

    /**
     * Tests valid compression value.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testBuildConfig_validCompressionType_succeeds(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name
        var serDeConfigs = GlueSchemaRegistryConfiguration(props)

        assertNotNull(serDeConfigs.compressionType)
        assertEquals(compressionType.name, serDeConfigs.compressionType.name)

        props[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType.name.lowercase()
        serDeConfigs = GlueSchemaRegistryConfiguration(props)

        assertNotNull(serDeConfigs.compressionType)
        assertEquals(compressionType.name, serDeConfigs.compressionType.name)
    }

    /**
     * Tests invalid compression value.
     */
    @Test
    fun testBuildConfig_invalidCompressionType_throwsException() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = "Random String"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertTrue(exception.message!!.contains("Invalid Compression type"))
    }

    /**
     * Tests the invalid compression message lists the accepted values.
     */
    @Test
    fun testBuildConfig_invalidCompressionType_listsTheAcceptedValues() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = "Random String"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertEquals(
            "Invalid Compression type : Random String, Accepted values are : NONE, ZLIB",
            exception.message,
        )
    }

    /**
     * Tests a non-String configuration value is rejected by name rather than by ClassCastException.
     */
    @Test
    fun testBuildConfig_cacheSizeAsInt_throwsNamedException() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.CACHE_SIZE] = 200

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.CACHE_SIZE} must be a String, " +
                "not a java.lang.Integer",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_jacksonFeaturesAsLists_areEnabled() {
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] =
            listOf(SerializationFeature.INDENT_OUTPUT.name)
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            listOf(DeserializationFeature.FAIL_ON_TRAILING_TOKENS.name)

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(listOf(SerializationFeature.INDENT_OUTPUT), configuration.jacksonSerializationFeatures)
        assertEquals(
            listOf(DeserializationFeature.FAIL_ON_TRAILING_TOKENS),
            configuration.jacksonDeserializationFeatures,
        )
        assertNull(configuration.jacksonSerializationFeatureToggles)
        assertNull(configuration.jacksonDeserializationFeatureToggles)
    }

    @Test
    fun testBuildConfig_jacksonFeaturesAsMaps_areToggles() {
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] =
            mapOf(SerializationFeature.INDENT_OUTPUT.name to true)
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name to false)

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(
            mapOf(SerializationFeature.INDENT_OUTPUT to true),
            configuration.jacksonSerializationFeatureToggles,
        )
        assertEquals(
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES to false),
            configuration.jacksonDeserializationFeatureToggles,
        )
        assertNull(configuration.jacksonSerializationFeatures)
        assertNull(configuration.jacksonDeserializationFeatures)
    }

    @Test
    fun testBuildConfig_jacksonFeatureToggleAsString_isAccepted() {
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name to "false")

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES to false),
            configuration.jacksonDeserializationFeatureToggles,
        )
    }

    @Test
    fun testBuildConfig_jacksonFeaturesAsNeitherListNorMap_throwsNamedException() {
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] = SerializationFeature.INDENT_OUTPUT.name

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Jackson Serialization features should be a list of names, or a map of name to boolean",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_jacksonFeatureToggleWithNonBooleanValue_throwsNamedException() {
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name to 0)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES} " +
                "must only map to a Boolean, or to \"true\" or \"false\"; got a java.lang.Integer",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_jacksonFeatureToggleWithUnrecognisedString_namesTheValue() {
        configs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] =
            mapOf(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES.name to "yes")

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES} " +
                "must only map to a Boolean, or to \"true\" or \"false\"; got the String \"yes\"",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_jacksonFeatureToggleWithNonStringKey_throwsNamedException() {
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] = mapOf(1 to true)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES} " +
                "must only contain String entries, not a java.lang.Integer",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_avroReaderSchema_isKeptAsGiven() {
        configs[AWSSchemaRegistryConstants.AVRO_READER_SCHEMA] = userReaderSchema

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(userReaderSchema, configuration.avroReaderSchema)
    }

    @Test
    fun testBuildConfig_noAvroReaderSchema_leavesItUnset() {
        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertNull(configuration.avroReaderSchema)
    }

    @Test
    fun testBuildConfig_avroReaderSchemaNotParseable_throwsNamedException() {
        configs[AWSSchemaRegistryConstants.AVRO_READER_SCHEMA] = """{"type":"record"}"""

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertTrue(
            exception.message!!.startsWith(
                "Configuration property ${AWSSchemaRegistryConstants.AVRO_READER_SCHEMA} is not a valid Avro schema",
            ),
            "Unexpected message: ${exception.message}",
        )
    }

    @Test
    fun testBuildConfig_avroReaderSchemaNotAString_throwsNamedException() {
        configs[AWSSchemaRegistryConstants.AVRO_READER_SCHEMA] = 1

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.AVRO_READER_SCHEMA} must be a String, " +
                "not a java.lang.Integer",
            exception.message,
        )
    }

    /**
     * Tests valid configuration tags value.
     */
    @Test
    fun testBuildConfig_validTags_succeeds() {
        val testTags = HashMap<String, String>()
        testTags["testTagKey"] = "testTagValue"
        testTags["testTagKey2"] = "testTagValue2"

        configs[AWSSchemaRegistryConstants.TAGS] = testTags
        val serDeConfigs = GlueSchemaRegistryConfiguration(configs)

        assertNotNull(serDeConfigs.tags)
        assertTrue(serDeConfigs.tags.containsKey("testTagKey"))
        assertTrue(serDeConfigs.tags.containsKey("testTagKey2"))

        assertEquals("testTagValue", serDeConfigs.tags["testTagKey"])
        assertEquals("testTagValue2", serDeConfigs.tags["testTagKey2"])
    }

    /**
     * Tests valid configuration tags value by building config from properties.
     */
    @Test
    fun testBuildConfigWithProperties_validTags_succeeds() {
        val props = createTestProperties()
        val testTags = HashMap<String, String>()
        testTags["testTagKey"] = "testTagValue"
        testTags["testTagKey2"] = "testTagValue2"

        props[AWSSchemaRegistryConstants.TAGS] = testTags
        val serDeConfigs = GlueSchemaRegistryConfiguration(props)

        assertNotNull(serDeConfigs.tags)

        assertTrue(serDeConfigs.tags.containsKey("testTagKey"))
        assertTrue(serDeConfigs.tags.containsKey("testTagKey2"))

        assertEquals("testTagValue", serDeConfigs.tags["testTagKey"])
        assertEquals("testTagValue2", serDeConfigs.tags["testTagKey2"])
    }

    /**
     * Tests invalid tag value.
     */
    @Test
    fun testBuildConfigWithProperties_invalidTags_throwsException() {
        val invalidMapString = "invalidTagString"
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.TAGS] = invalidMapString

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }
        assertTrue(exception.message!!.contains(AWSSchemaRegistryConstants.TAGS_CONFIG_NOT_HASHMAP_MSG))
    }

    /**
     * Tests invalid metadata value.
     */
    @Test
    fun testBuildAWSAvroSerializer_invalidMetadata_throwsException() {
        val metadata = ArrayList<String>()
        metadata.add("default-topic")
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.METADATA] = metadata

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }

        assertTrue(exception.message!!.contains("The metadata instance is not a hash map"))
    }

    /**
     * Tests autogenerated description string
     */
    @Test
    fun testValidateAndSetDescription_withoutDescriptionConfig_succeeds() {
        val props = createTestProperties()
        props.remove(AWSSchemaRegistryConstants.DESCRIPTION)
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertNotNull(glueSchemaRegistryConfiguration.description)
    }

    /**
     * Tests custom description string
     */
    @Test
    fun testValidateAndSetDescription_withDescriptionConfig_succeeds() {
        val props = createTestProperties()
        val expectedDescription = "test-description"
        props[AWSSchemaRegistryConstants.DESCRIPTION] = expectedDescription
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertEquals(expectedDescription, glueSchemaRegistryConfiguration.description)
    }

    /**
     * Tests default registry name
     */
    @Test
    fun testValidateAndSetRegistryName_withoutRegistryConfig_throwsException() {
        val props = createTestProperties()
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertEquals(
            AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME,
            glueSchemaRegistryConfiguration.registryName,
        )
    }

    /**
     * Tests custom registry name
     */
    @Test
    fun testValidateAndSetRegistryName_withRegistryConfig_throwsException() {
        val expectedRegistryName = "test-registry"
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.REGISTRY_NAME] = expectedRegistryName
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertEquals(expectedRegistryName, glueSchemaRegistryConfiguration.registryName)
    }

    /**
     * Tests valid proxy URL value.
     */
    @Test
    fun testBuildConfig_validProxyUrl_success() {
        val props = createTestProperties()
        val proxy = "http://proxy.servers.url:8080"
        props[AWSSchemaRegistryConstants.PROXY_URL] = proxy
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)
        assertEquals(URI.create(proxy), glueSchemaRegistryConfiguration.proxyUrl)
    }

    /**
     * Tests invalid proxy URL value.
     */
    @Test
    fun testBuildConfig_invalidProxyUrl_throwsException() {
        val props = createTestProperties()
        val proxy = "http:// proxy.url: 8080"
        props[AWSSchemaRegistryConstants.PROXY_URL] = "http:// proxy.url: 8080"
        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(props) }
        assertEquals("Proxy URL property is not a valid URL: $proxy", exception.message)
    }

    /**
     * Tests that JSON class name resolution defaults to disabled (secure default) when not configured.
     */
    @Test
    fun testJsonClassNameResolution_withoutConfig_defaultsToDisabled() {
        val props = createTestProperties()
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertFalse(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled)
    }

    /**
     * Tests that customers can opt in to JSON class name resolution.
     */
    @Test
    fun testJsonClassNameResolution_setToTrue_isEnabled() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "true"
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertTrue(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled)
    }

    /**
     * Tests that JSON class name resolution can be explicitly disabled.
     */
    @Test
    fun testJsonClassNameResolution_setToFalse_isDisabled() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "false"
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertFalse(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled)
    }

    @Test
    fun testJsonSchemaNullable_withoutConfig_defaultsToDisabled() {
        val props = createTestProperties()
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertFalse(glueSchemaRegistryConfiguration.isJsonSchemaNullableEnabled)
    }

    @Test
    fun testJsonSchemaNullable_setToTrue_isEnabled() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED] = "true"
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertTrue(glueSchemaRegistryConfiguration.isJsonSchemaNullableEnabled)
    }

    @Test
    fun testJsonSchemaNullable_setToFalse_isDisabled() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED] = "false"
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(props)

        assertFalse(glueSchemaRegistryConfiguration.isJsonSchemaNullableEnabled)
    }

    /**
     * Tests that the JSON class name allowlist is parsed correctly from a comma-separated string.
     */
    @Test
    fun testJsonClassNameAllowlist_commaSeparated_isParsedCorrectly() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = "com.example.Foo, com.example.Bar"
        val config = GlueSchemaRegistryConfiguration(props)

        assertEquals(2, config.jsonClassNameAllowlist!!.size)
        assertTrue(config.jsonClassNameAllowlist!!.contains("com.example.Foo"))
        assertTrue(config.jsonClassNameAllowlist!!.contains("com.example.Bar"))
    }

    /**
     * Tests that the allowlist defaults to empty when not configured.
     */
    @Test
    fun testJsonClassNameAllowlist_notConfigured_defaultsToEmpty() {
        val props = createTestProperties()
        val config = GlueSchemaRegistryConfiguration(props)

        assertNotNull(config.jsonClassNameAllowlist)
        assertTrue(config.jsonClassNameAllowlist!!.isEmpty())
    }

    /**
     * Tests that the allowlist handles a single class correctly.
     */
    @Test
    fun testJsonClassNameAllowlist_singleClass_isParsedCorrectly() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = "com.example.SingleClass"
        val config = GlueSchemaRegistryConfiguration(props)

        assertEquals(1, config.jsonClassNameAllowlist!!.size)
        assertTrue(config.jsonClassNameAllowlist!!.contains("com.example.SingleClass"))
    }

    /**
     * Tests that stray commas do not put an empty, never-matching entry into the allowlist.
     */
    @Test
    fun testJsonClassNameAllowlist_strayCommas_areIgnored() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = ",com.example.Foo,,com.example.Bar, ,"
        val config = GlueSchemaRegistryConfiguration(props)

        assertEquals(2, config.jsonClassNameAllowlist!!.size)
        assertTrue(config.jsonClassNameAllowlist!!.contains("com.example.Foo"))
        assertTrue(config.jsonClassNameAllowlist!!.contains("com.example.Bar"))
        assertFalse(config.jsonClassNameAllowlist!!.contains(""))
    }

    /**
     * Tests that an allowlist of only separators and whitespace leaves the default empty allowlist.
     */
    @Test
    fun testJsonClassNameAllowlist_onlySeparators_defaultsToEmpty() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = " , , "
        val config = GlueSchemaRegistryConfiguration(props)

        assertTrue(config.jsonClassNameAllowlist!!.isEmpty())
    }

    /**
     * Tests that a value that is neither "true" nor "false" leaves resolution disabled, which is
     * the safe direction for a security opt-in.
     */
    @Test
    fun testJsonClassNameResolution_unrecognizedValue_isDisabled() {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] = "ture"
        val config = GlueSchemaRegistryConfiguration(props)

        assertFalse(config.isJsonClassNameResolutionEnabled)
    }

    /**
     * Builds a configuration whose allowlist is the given comma-separated value.
     */
    private fun configWithAllowlist(allowlist: String): GlueSchemaRegistryConfiguration {
        val props = createTestProperties()
        props[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST] = allowlist
        return GlueSchemaRegistryConfiguration(props)
    }

    /**
     * Tests that an exact allowlist entry allows that class and nothing else.
     */
    @Test
    fun testIsClassNameAllowed_exactEntry_matchesOnlyThatClass() {
        val config = configWithAllowlist("com.example.pojos.Car")

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"))
        assertFalse(config.isClassNameAllowed("com.example.pojos.Truck"))
    }

    /**
     * Tests that a package entry allows classes directly in that package.
     */
    @Test
    fun testIsClassNameAllowed_packageEntry_matchesDirectMembers() {
        val config = configWithAllowlist("com.example.pojos.*")

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"))
        assertTrue(config.isClassNameAllowed("com.example.pojos.Truck"))
    }

    /**
     * Tests that a package entry does not reach into sub-packages. A nested package is a
     * separate decision from the one the operator made.
     */
    @Test
    fun testIsClassNameAllowed_packageEntry_doesNotMatchSubPackages() {
        val config = configWithAllowlist("com.example.pojos.*")

        assertFalse(config.isClassNameAllowed("com.example.pojos.nested.Car"))
    }

    /**
     * Tests that the trailing dot is part of the prefix, so a package entry cannot match a
     * sibling package that merely starts with the same characters.
     */
    @Test
    fun testIsClassNameAllowed_packageEntry_doesNotMatchPrefixSiblingPackage() {
        val config = configWithAllowlist("com.example.pojos.*")

        assertFalse(config.isClassNameAllowed("com.example.pojosX.Car"))
        assertFalse(config.isClassNameAllowed("com.example.pojos"))
    }

    /**
     * Tests that a nested class of an allowed package matches, since it is declared inside a
     * class the entry already allows.
     */
    @Test
    fun testIsClassNameAllowed_packageEntry_matchesNestedClass() {
        val config = configWithAllowlist("com.example.pojos.*")

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car\$Engine"))
    }

    /**
     * Tests that entries are matched literally rather than as regular expressions, so regex
     * metacharacters in an entry do not widen what it matches.
     */
    @Test
    fun testIsClassNameAllowed_entryIsNotTreatedAsRegex() {
        val config = configWithAllowlist("com.example.pojos.Ca.")

        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"))
    }

    /**
     * Tests that exact and package entries coexist in one allowlist.
     */
    @Test
    fun testIsClassNameAllowed_mixedEntries_bothKindsMatch() {
        val config = configWithAllowlist("com.example.Legacy, com.example.pojos.*")

        assertTrue(config.isClassNameAllowed("com.example.Legacy"))
        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"))
        assertFalse(config.isClassNameAllowed("com.example.other.Car"))
    }

    /**
     * Tests that the default empty allowlist permits nothing, and that a null class name is
     * rejected rather than throwing.
     */
    @Test
    fun testIsClassNameAllowed_emptyAllowlistAndNull_areRejected() {
        val config = GlueSchemaRegistryConfiguration(createTestProperties())

        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"))
        assertFalse(config.isClassNameAllowed(null))
    }

    /**
     * Tests that a bare wildcard is rejected. Allowing every class on the classpath is the
     * behavior the allowlist exists to prevent, so it must not be reachable in one character.
     */
    @Test
    fun testJsonClassNameAllowlist_bareWildcard_throwsException() {
        assertThrows(AWSSchemaRegistryException::class.java) { configWithAllowlist("*") }
        assertThrows(AWSSchemaRegistryException::class.java) { configWithAllowlist(".*") }
        assertThrows(AWSSchemaRegistryException::class.java) { configWithAllowlist("com.example.pojos.Car,*") }
        // Whitespace must not smuggle a bare wildcard past the check.
        assertThrows(AWSSchemaRegistryException::class.java) { configWithAllowlist(" * ") }
        assertThrows(AWSSchemaRegistryException::class.java) { configWithAllowlist("com.example.Car, .* ") }
    }

    /**
     * Tests that wildcard-looking entries which are not a bare wildcard match nothing rather than
     * matching broadly. They are treated as literal class names, so they fail closed.
     */
    @Test
    fun testIsClassNameAllowed_malformedWildcards_matchNothing() {
        assertFalse(configWithAllowlist("**").isClassNameAllowed("com.example.Car"))
        assertFalse(configWithAllowlist("com.example.*.Car").isClassNameAllowed("com.example.pojos.Car"))
        assertFalse(configWithAllowlist("com.example*").isClassNameAllowed("com.example.Car"))
        assertFalse(configWithAllowlist("com.example.pojos*").isClassNameAllowed("com.example.pojos.Car"))
    }

    /**
     * Tests that even a top-level package entry stays narrow, since only direct members match.
     * `"com.*"` therefore cannot stand in for a bare wildcard.
     */
    @Test
    fun testIsClassNameAllowed_topLevelPackageEntry_staysNarrow() {
        val config = configWithAllowlist("com.*")

        assertTrue(config.isClassNameAllowed("com.Car"))
        assertFalse(config.isClassNameAllowed("com.example.Car"))
        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"))
    }

    /**
     * Tests that a bare wildcard installed through the generated setter matches nothing. The
     * parsing-time rejection does not cover this path, so the rule has to hold at the point of use
     * as well.
     */
    @Test
    fun testIsClassNameAllowed_bareWildcardViaSetter_matchesNothing() {
        val config = GlueSchemaRegistryConfiguration(createTestProperties())

        config.jsonClassNameAllowlist = HashSet(listOf("*"))
        assertFalse(config.isClassNameAllowed("com.example.Car"))

        config.jsonClassNameAllowlist = HashSet(listOf(".*"))
        assertFalse(config.isClassNameAllowed("com.example.Car"))

        // A scoped package entry set the same way still works, so the guard is not over-broad.
        config.jsonClassNameAllowlist = HashSet(listOf("com.example.*"))
        assertTrue(config.isClassNameAllowed("com.example.Car"))
    }

    @Test
    fun testBuildConfig_objectMapperFactoryAndModuleUnset_leaveThemNull() {
        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertNull(configuration.objectMapperFactory)
        assertNull(configuration.registerJavaTimeModule)
    }

    /**
     * The mapper the two JSON serdes built for themselves registered no module and read numbers
     * into exact `BigDecimal` nodes, which is what keeps the scale of a decimal rather than
     * normalising it. Nothing about that changes while the two new keys are unset.
     */
    @Test
    fun testBuildObjectMapper_noCustomisation_matchesTheMapperTheSerdesUsedToBuild() {
        val configuration = GlueSchemaRegistryConfiguration(configs)

        val objectMapper = configuration.buildObjectMapper()

        assertEquals(
            EXACT_DECIMAL,
            objectMapper.nodeFactory
                .numberNode(BigDecimal(EXACT_DECIMAL))
                .decimalValue()
                .toString(),
        )
        assertTrue(objectMapper.registeredModuleIds.isEmpty())
    }

    @Test
    fun testBuildObjectMapper_calledTwice_returnsDistinctMappers() {
        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertNotSame(configuration.buildObjectMapper(), configuration.buildObjectMapper())
    }

    @Test
    fun testBuildConfig_registerJavaTimeModuleAsString_isKept() {
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = JavaTimeModule::class.java.name

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(JavaTimeModule::class.java.name, configuration.registerJavaTimeModule)
        assertTrue(
            configuration.buildObjectMapper().registeredModuleIds.contains(JavaTimeModule().typeId),
        )
    }

    @Test
    fun testBuildConfig_registerJavaTimeModuleAsClass_isKeptByName() {
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = JavaTimeModule::class.java

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(JavaTimeModule::class.java.name, configuration.registerJavaTimeModule)
    }

    @Test
    fun testBuildConfig_registerJavaTimeModuleNotOnTheClasspath_namesTheClass() {
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = "com.example.NoSuchModule"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE} names a class " +
                "that could not be instantiated: com.example.NoSuchModule. It has to be a public class with " +
                "a public no-argument constructor, implementing com.fasterxml.jackson.databind.Module, and " +
                "on the classpath.",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_registerJavaTimeModuleNamingSomethingElse_namesTheClass() {
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = ObjectMapper::class.java.name

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE} has to name a " +
                "class implementing com.fasterxml.jackson.databind.Module; " +
                "com.fasterxml.jackson.databind.ObjectMapper does not.",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_registerJavaTimeModuleAsNeitherStringNorClass_namesTheType() {
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = 1

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE} must be a class " +
                "name, or a Class, not a java.lang.Integer",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_objectMapperFactory_buildsTheMapperItReturns() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = SortingObjectMapperFactory::class.java.name

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(SortingObjectMapperFactory::class.java.name, configuration.objectMapperFactory)
        assertTrue(
            configuration.buildObjectMapper().isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY),
        )
    }

    @Test
    fun testBuildConfig_objectMapperFactoryAsClass_isKeptByName() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = SortingObjectMapperFactory::class.java

        val configuration = GlueSchemaRegistryConfiguration(configs)

        assertEquals(SortingObjectMapperFactory::class.java.name, configuration.objectMapperFactory)
    }

    @Test
    fun testBuildConfig_objectMapperFactoryNotOnTheClasspath_namesTheClass() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = "com.example.NoSuchFactory"

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY} names a class that " +
                "could not be instantiated: com.example.NoSuchFactory. It has to be a public class with a " +
                "public no-argument constructor, implementing " +
                "com.amazonaws.services.schemaregistry.common.configs.ObjectMapperFactory, and on the classpath.",
            exception.message,
        )
    }

    @Test
    fun testBuildConfig_objectMapperFactoryWithoutANoArgConstructor_namesTheClass() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = ConstructorTakingObjectMapperFactory::class.java.name

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertTrue(
            exception.message!!.startsWith(
                "Configuration property ${AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY} names a class " +
                    "that could not be instantiated: ${ConstructorTakingObjectMapperFactory::class.java.name}.",
            ),
        )
    }

    @Test
    fun testBuildConfig_objectMapperFactoryNamingSomethingElse_namesTheClass() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = ObjectMapper::class.java.name

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) { GlueSchemaRegistryConfiguration(configs) }

        assertEquals(
            "Configuration property ${AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY} has to name a class " +
                "implementing com.amazonaws.services.schemaregistry.common.configs.ObjectMapperFactory; " +
                "com.fasterxml.jackson.databind.ObjectMapper does not.",
            exception.message,
        )
    }

    /**
     * Tests the documented order: the factory builds the mapper, the module is registered on it,
     * and the feature properties are applied last, so a feature named in both wins there.
     */
    @Test
    fun testBuildObjectMapper_featurePropertyAndFactoryDisagree_theFeaturePropertyWins() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = IndentingObjectMapperFactory::class.java.name
        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = JavaTimeModule::class.java.name
        configs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] =
            mapOf(SerializationFeature.INDENT_OUTPUT.name to false)

        val objectMapper = GlueSchemaRegistryConfiguration(configs).buildObjectMapper()

        assertFalse(objectMapper.isEnabled(SerializationFeature.INDENT_OUTPUT))
        assertTrue(objectMapper.registeredModuleIds.contains(JavaTimeModule().typeId))
    }

    @Test
    fun testBuildObjectMapper_factoryOnly_keepsWhatTheFactorySet() {
        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = IndentingObjectMapperFactory::class.java.name

        val objectMapper = GlueSchemaRegistryConfiguration(configs).buildObjectMapper()

        assertTrue(objectMapper.isEnabled(SerializationFeature.INDENT_OUTPUT))
    }

    @Test
    fun testEquals_configurationsDifferingByTheNewKeys_areNotEqual() {
        val plain = GlueSchemaRegistryConfiguration(HashMap(configs))

        configs[AWSSchemaRegistryConstants.REGISTER_JAVA_TIME_MODULE] = JavaTimeModule::class.java.name
        val withModule = GlueSchemaRegistryConfiguration(HashMap(configs))

        configs[AWSSchemaRegistryConstants.OBJECT_MAPPER_FACTORY] = SortingObjectMapperFactory::class.java.name
        val withFactory = GlueSchemaRegistryConfiguration(HashMap(configs))

        assertNotEquals(plain, withModule)
        assertNotEquals(withModule, withFactory)
        assertEquals(withFactory, GlueSchemaRegistryConfiguration(HashMap(configs)))
        assertTrue(withFactory.toString().contains(SortingObjectMapperFactory::class.java.name))
        assertTrue(withFactory.toString().contains(JavaTimeModule::class.java.name))
    }

    class SortingObjectMapperFactory : ObjectMapperFactory {
        override fun newObjectMapper(): ObjectMapper = JsonMapper
            .builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .build()
    }

    class IndentingObjectMapperFactory : ObjectMapperFactory {
        override fun newObjectMapper(): ObjectMapper = DefaultObjectMapperFactory()
            .newObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
    }

    class ConstructorTakingObjectMapperFactory(
        private val objectMapper: ObjectMapper,
    ) : ObjectMapperFactory {
        override fun newObjectMapper(): ObjectMapper = objectMapper
    }

    private companion object {
        const val EXACT_DECIMAL = "1.2000"
    }
}
