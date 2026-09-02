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
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.commons.lang3.EnumUtils
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.glue.model.Compatibility
import java.net.URI
import java.util.Properties

/**
 * Glue Schema Registry Configuration entries.
 */
class GlueSchemaRegistryConfiguration {
    var compressionType: AWSSchemaRegistryConstants.COMPRESSION = AWSSchemaRegistryConstants.COMPRESSION.NONE
    var endPoint: String? = null
    var region: String? = null
    var timeToLiveMillis: Long = AWSSchemaRegistryConstants.DEFAULT_CACHE_TIME_TO_LIVE_MILLIS
    var cacheSize: Int = AWSSchemaRegistryConstants.DEFAULT_CACHE_SIZE
    var avroRecordType: AvroRecordType? = AvroRecordType.GENERIC_RECORD
    var protobufMessageType: ProtobufMessageType? = null
    var registryName: String? = null
    var compatibilitySetting: Compatibility? = null
    var description: String? = null

    // Named isXxx so the generated accessors stay isXxx()/setXxx(), matching what Lombok
    // produced — that is the name Java callers use.
    var isSchemaAutoRegistrationEnabled: Boolean = false
    var isJsonClassNameResolutionEnabled: Boolean = false
    var isJsonSchemaNullableEnabled: Boolean = false
    var isJsonSchemaCompatibilityCheckEnabled: Boolean = false

    var jsonClassNameAllowlist: Set<String>? = emptySet()
    var tags: Map<String, String> = HashMap()
    var metadata: Map<String, String>? = null
    var secondaryDeserializer: String? = null
    var proxyUrl: URI? = null

    /**
     * Name of the application using the serializer/deserializer.
     * Ex: Kafka, KafkaConnect, KPL etc.
     */
    var userAgentApp: String? = "default"

    var avroReaderSchema: String? = null

    var jacksonSerializationFeatures: List<SerializationFeature>? = null
    var jacksonDeserializationFeatures: List<DeserializationFeature>? = null
    var jacksonSerializationFeatureToggles: Map<SerializationFeature, Boolean>? = null
    var jacksonDeserializationFeatureToggles: Map<DeserializationFeature, Boolean>? = null

    constructor(region: String?) {
        val config = HashMap<String, Any?>()
        config[AWSSchemaRegistryConstants.AWS_REGION] = region
        buildConfigs(config)
    }

    constructor(configs: Map<String, *>) {
        buildConfigs(configs)
    }

    constructor(properties: Properties) {
        buildConfigs(getMapFromPropertiesFile(properties))
    }

    private fun buildConfigs(configs: Map<String, *>) {
        buildSchemaRegistryConfigs(configs)
        buildCacheConfigs(configs)
    }

    private fun buildSchemaRegistryConfigs(configs: Map<String, *>) {
        validateAndSetAWSRegion(configs)
        validateAndSetAWSEndpoint(configs)
        validateAndSetRegistryName(configs)
        validateAndSetDescription(configs)
        validateAndSetAvroRecordType(configs)
        validateAndSetAvroReaderSchema(configs)
        validateAndSetProtobufMessageType(configs)
        validateAndSetCompatibility(configs)
        validateAndSetCompressionType(configs)
        validateAndSetSchemaAutoRegistrationSetting(configs)
        validateAndSetJsonClassNameResolutionSetting(configs)
        validateAndSetJsonSchemaNullableSetting(configs)
        validateAndSetJsonSchemaCompatibilityCheckSetting(configs)
        validateAndSetJacksonSerializationFeatures(configs)
        validateAndSetJacksonDeserializationFeatures(configs)
        validateAndSetTags(configs)
        validateAndSetMetadata(configs)
        validateAndSetUserAgent(configs)
        validateAndSetSecondaryDeserializer(configs)
        validateAndSetProxyUrl(configs)
    }

    private fun buildCacheConfigs(configs: Map<String, *>) {
        validateAndSetCacheSize(configs)
        validateAndSetCacheTTL(configs)
    }

    private fun validateAndSetSecondaryDeserializer(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)) {
            when (val value = configs[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER]) {
                is String -> secondaryDeserializer = value
                is Class<*> -> secondaryDeserializer = value.name
                else -> throw AWSSchemaRegistryException("Invalid secondary de-serializer configuration")
            }
        }
    }

    private fun validateAndSetUserAgent(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.USER_AGENT_APP)) {
            userAgentApp = nullableStringConfig(configs, AWSSchemaRegistryConstants.USER_AGENT_APP)
        }
    }

    private fun validateAndSetCompressionType(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.COMPRESSION_TYPE)) {
            val value = stringConfig(configs, AWSSchemaRegistryConstants.COMPRESSION_TYPE)
            if (validateCompressionType(value)) {
                compressionType = AWSSchemaRegistryConstants.COMPRESSION.valueOf(value.uppercase())
            }
        }
    }

    private fun validateCompressionType(compressionType: String): Boolean {
        if (!EnumUtils.isValidEnum(AWSSchemaRegistryConstants.COMPRESSION::class.java, compressionType.uppercase())) {
            throw AWSSchemaRegistryException(
                "Invalid Compression type : $compressionType, Accepted values are : " +
                    AWSSchemaRegistryConstants.COMPRESSION.entries.joinToString(),
            )
        }
        return true
    }

    private fun validateAndSetAWSRegion(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_REGION)) {
            region = configs[AWSSchemaRegistryConstants.AWS_REGION].toString()
        } else {
            region =
                try {
                    DefaultAwsRegionProviderChain.builder().build().region.id()
                } catch (ex: SdkClientException) {
                    throw AWSSchemaRegistryException("Region is not defined in the properties", ex)
                }
        }
    }

    private fun validateAndSetCompatibility(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.COMPATIBILITY_SETTING)) {
            compatibilitySetting =
                Compatibility.fromValue(
                    configs[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING].toString().uppercase(),
                )

            if (compatibilitySetting == null || compatibilitySetting == Compatibility.UNKNOWN_TO_SDK_VERSION) {
                throw AWSSchemaRegistryException(
                    "Invalid compatibility setting : " +
                        "${configs[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING]}, " +
                        "Accepted values are : ${Compatibility.knownValues()}",
                )
            }
        } else {
            compatibilitySetting = AWSSchemaRegistryConstants.DEFAULT_COMPATIBILITY_SETTING
        }
    }

    private fun validateAndSetRegistryName(configs: Map<String, *>) {
        registryName =
            if (isPresent(configs, AWSSchemaRegistryConstants.REGISTRY_NAME)) {
                configs[AWSSchemaRegistryConstants.REGISTRY_NAME].toString()
            } else {
                AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME
            }
    }

    private fun validateAndSetAWSEndpoint(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AWS_ENDPOINT)) {
            endPoint = configs[AWSSchemaRegistryConstants.AWS_ENDPOINT].toString()
        }
    }

    private fun validateAndSetProxyUrl(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.PROXY_URL)) {
            val value = stringConfig(configs, AWSSchemaRegistryConstants.PROXY_URL)
            try {
                proxyUrl = URI.create(value)
            } catch (e: IllegalArgumentException) {
                throw AWSSchemaRegistryException("Proxy URL property is not a valid URL: $value", e)
            }
        }
    }

    private fun validateAndSetDescription(configs: Map<String, *>) {
        description =
            if (isPresent(configs, AWSSchemaRegistryConstants.DESCRIPTION)) {
                configs[AWSSchemaRegistryConstants.DESCRIPTION].toString()
            } else {
                buildDescriptionFromProperties()
            }
    }

    private fun validateAndSetCacheSize(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.CACHE_SIZE)) {
            val value = stringConfig(configs, AWSSchemaRegistryConstants.CACHE_SIZE)
            cacheSize =
                try {
                    value.toInt()
                } catch (e: NumberFormatException) {
                    throw AWSSchemaRegistryException("Cache size property is not a valid size : $value", e)
                }
        } else {
            log.info("Cache Size is not found, using default {}", cacheSize)
        }
    }

    private fun validateAndSetCacheTTL(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS)) {
            val value = stringConfig(configs, AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS)
            timeToLiveMillis =
                try {
                    value.toLong()
                } catch (e: NumberFormatException) {
                    throw AWSSchemaRegistryException("Time to live cache property is not a valid time : $value", e)
                }
        } else {
            log.info("Cache Time to live is not found, using default {}", timeToLiveMillis)
        }
    }

    private fun validateAndSetAvroRecordType(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AVRO_RECORD_TYPE)) {
            avroRecordType =
                AvroRecordType.valueOf(stringConfig(configs, AWSSchemaRegistryConstants.AVRO_RECORD_TYPE))
        }
    }

    private fun validateAndSetProtobufMessageType(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE)) {
            protobufMessageType =
                ProtobufMessageType.valueOf(
                    stringConfig(configs, AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE),
                )
        }
    }

    private fun validateAndSetSchemaAutoRegistrationSetting(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING)) {
            isSchemaAutoRegistrationEnabled =
                configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING].toString().toBoolean()
        } else {
            log.info(
                "schemaAutoRegistrationEnabled is not defined in the properties. Using the default value {}",
                isSchemaAutoRegistrationEnabled,
            )
        }
    }

    private fun validateAndSetJsonClassNameResolutionSetting(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED)) {
            isJsonClassNameResolutionEnabled =
                booleanConfig(configs, AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED)
        } else {
            log.info(
                "jsonClassNameResolutionEnabled is not defined in the properties. Using the default value {}",
                isJsonClassNameResolutionEnabled,
            )
        }

        if (isPresent(configs, AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST)) {
            // Accept either a comma-separated String or a List, matching how the other collection
            // configs are supplied. Anything else would otherwise fall through to toString() and
            // silently produce a single never-matching entry.
            val rawEntries: Sequence<String> =
                when (val allowlistValue = configs[AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST]) {
                    is List<*> -> allowlistValue.asSequence().map { it.toString() }
                    is String -> allowlistValue.split(",").asSequence()
                    else -> throw AWSSchemaRegistryException(
                        "${AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST} must be a comma-separated " +
                            "String or a List of class names.",
                    )
                }
            // Drop empty entries so that leading, trailing or doubled commas do not put a
            // never-matching "" into the allowlist.
            val allowedClassNames = rawEntries.map { it.trim() }.filter { it.isNotEmpty() }.toSet()
            // A bare "*" would allow every class on the classpath, which is the behavior this
            // allowlist exists to prevent.
            if (allowedClassNames.contains("*") || allowedClassNames.contains(PACKAGE_WILDCARD_SUFFIX)) {
                throw AWSSchemaRegistryException(
                    "${AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST} must not contain a bare wildcard. " +
                        "List classes explicitly, or scope a package with a prefix such as \"com.example.pojos.*\".",
                )
            }
            if (allowedClassNames.isNotEmpty()) {
                jsonClassNameAllowlist = allowedClassNames
            }
        }
    }

    /**
     * Whether the JSON deserializer may instantiate [className], given the configured allowlist.
     * An entry matches either exactly, or as a package when it ends in `".*"`.
     *
     * Matching is a literal prefix test rather than a regular expression: configuration-supplied
     * patterns would otherwise have to be trusted not to match more than intended.
     */
    fun isClassNameAllowed(className: String?): Boolean {
        val allowlist = jsonClassNameAllowlist
        if (className == null || allowlist == null) {
            return false
        }
        if (allowlist.contains(className)) {
            return true
        }
        return allowlist
            .asSequence()
            .filter { it.endsWith(PACKAGE_WILDCARD_SUFFIX) }
            .filter { !isBareWildcard(it) }
            .any { isDirectlyInPackage(className, it) }
    }

    private fun validateAndSetTags(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.TAGS)) {
            val value = configs[AWSSchemaRegistryConstants.TAGS]
            if (value is HashMap<*, *>) {
                @Suppress("UNCHECKED_CAST")
                tags = value as Map<String, String>
            } else {
                throw AWSSchemaRegistryException(AWSSchemaRegistryConstants.TAGS_CONFIG_NOT_HASHMAP_MSG)
            }
        } else {
            log.info("Tags value is not defined in the properties. No tags are assigned")
        }
    }

    private fun validateAndSetMetadata(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.METADATA)) {
            val value = configs[AWSSchemaRegistryConstants.METADATA]
            if (value is HashMap<*, *>) {
                @Suppress("UNCHECKED_CAST")
                metadata = value as Map<String, String>
            } else {
                throw AWSSchemaRegistryException("The metadata instance is not a hash map")
            }
        }
    }

    private fun validateAndSetJsonSchemaNullableSetting(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED)) {
            isJsonSchemaNullableEnabled =
                booleanConfig(configs, AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED)
        }
    }

    private fun booleanConfig(
        configs: Map<String, *>,
        key: String,
    ): Boolean {
        val value = configs[key].toString()
        if (!value.equals("true", ignoreCase = true) && !value.equals("false", ignoreCase = true)) {
            log.warn("Unrecognized value '{}' for {}; interpreting it as false.", value, key)
        }
        return value.toBoolean()
    }

    private fun validateAndSetJsonSchemaCompatibilityCheckSetting(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED)) {
            isJsonSchemaCompatibilityCheckEnabled =
                booleanConfig(configs, AWSSchemaRegistryConstants.JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED)
        }
    }

    private fun validateAndSetAvroReaderSchema(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.AVRO_READER_SCHEMA)) {
            val definition = stringConfig(configs, AWSSchemaRegistryConstants.AVRO_READER_SCHEMA)
            try {
                org.apache.avro.Schema
                    .Parser()
                    .parse(definition)
            } catch (e: Exception) {
                throw AWSSchemaRegistryException(
                    "Configuration property ${AWSSchemaRegistryConstants.AVRO_READER_SCHEMA} is not a valid " +
                        "Avro schema: ${e.message}",
                    e,
                )
            }
            avroReaderSchema = definition
        }
    }

    private fun validateAndSetJacksonSerializationFeatures(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES)) {
            val key = AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES
            when (val value = configs[key]) {
                is List<*> ->
                    jacksonSerializationFeatures =
                        value.map { SerializationFeature.valueOf(stringEntry(key, it)) }

                is Map<*, *> ->
                    jacksonSerializationFeatureToggles =
                        featureToggles(key, value) { SerializationFeature.valueOf(it) }

                else -> throw AWSSchemaRegistryException(
                    "Jackson Serialization features should be a list of names, or a map of name to boolean",
                )
            }
        }
    }

    private fun validateAndSetJacksonDeserializationFeatures(configs: Map<String, *>) {
        if (isPresent(configs, AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES)) {
            val key = AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES
            when (val value = configs[key]) {
                is List<*> ->
                    jacksonDeserializationFeatures =
                        value.map { DeserializationFeature.valueOf(stringEntry(key, it)) }

                is Map<*, *> ->
                    jacksonDeserializationFeatureToggles =
                        featureToggles(key, value) { DeserializationFeature.valueOf(it) }

                else -> throw AWSSchemaRegistryException(
                    "Jackson Deserialization features should be a list of names, or a map of name to boolean",
                )
            }
        }
    }

    private fun <T> featureToggles(
        key: String,
        toggles: Map<*, *>,
        parse: (String) -> T,
    ): Map<T, Boolean> = toggles.entries.associate { (name, enabled) ->
        parse(stringEntry(key, name)) to booleanEntry(key, enabled)
    }

    private fun booleanEntry(
        key: String,
        entry: Any?,
    ): Boolean {
        if (entry is Boolean) {
            return entry
        }
        if (entry is String && (entry.equals("true", ignoreCase = true) || entry.equals("false", ignoreCase = true))) {
            return entry.toBoolean()
        }
        throw AWSSchemaRegistryException(
            "Configuration property $key must only map to a Boolean, or to \"true\" or \"false\"; " +
                "got ${describeValue(entry)}",
        )
    }

    private fun stringConfig(
        configs: Map<String, *>,
        key: String,
    ): String {
        val value = configs[key]
        if (value is String) {
            return value
        }
        throw AWSSchemaRegistryException("Configuration property $key must be a String, not ${describeType(value)}")
    }

    private fun nullableStringConfig(
        configs: Map<String, *>,
        key: String,
    ): String? = if (configs[key] == null) null else stringConfig(configs, key)

    private fun stringEntry(
        key: String,
        entry: Any?,
    ): String {
        if (entry is String) {
            return entry
        }
        throw AWSSchemaRegistryException(
            "Configuration property $key must only contain String entries, not ${describeType(entry)}",
        )
    }

    private fun describeType(value: Any?): String = if (value == null) "null" else "a ${value.javaClass.name}"

    private fun describeValue(value: Any?): String = if (value is String) "the String \"$value\"" else describeType(value)

    private fun isPresent(
        configs: Map<String, *>,
        key: String,
    ): Boolean {
        if (!GlueSchemaRegistryUtils.getInstance().checkIfPresentInMap(configs, key)) {
            log.info("{} key is not present in the configs", key)
            return false
        }
        return true
    }

    // @Data generated equals/hashCode/toString over every field. Two serializer-deserializer
    // tests compare two configurations by value: without these overrides the comparison
    // falls back to identity and fails.
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is GlueSchemaRegistryConfiguration) return false
        return compressionType == other.compressionType &&
            endPoint == other.endPoint &&
            region == other.region &&
            timeToLiveMillis == other.timeToLiveMillis &&
            cacheSize == other.cacheSize &&
            avroRecordType == other.avroRecordType &&
            avroReaderSchema == other.avroReaderSchema &&
            protobufMessageType == other.protobufMessageType &&
            registryName == other.registryName &&
            compatibilitySetting == other.compatibilitySetting &&
            description == other.description &&
            isSchemaAutoRegistrationEnabled == other.isSchemaAutoRegistrationEnabled &&
            isJsonClassNameResolutionEnabled == other.isJsonClassNameResolutionEnabled &&
            isJsonSchemaNullableEnabled == other.isJsonSchemaNullableEnabled &&
            isJsonSchemaCompatibilityCheckEnabled == other.isJsonSchemaCompatibilityCheckEnabled &&
            jsonClassNameAllowlist == other.jsonClassNameAllowlist &&
            tags == other.tags &&
            metadata == other.metadata &&
            secondaryDeserializer == other.secondaryDeserializer &&
            proxyUrl == other.proxyUrl &&
            userAgentApp == other.userAgentApp &&
            jacksonSerializationFeatures == other.jacksonSerializationFeatures &&
            jacksonDeserializationFeatures == other.jacksonDeserializationFeatures &&
            jacksonSerializationFeatureToggles == other.jacksonSerializationFeatureToggles &&
            jacksonDeserializationFeatureToggles == other.jacksonDeserializationFeatureToggles
    }

    override fun hashCode(): Int = listOf(
        compressionType, endPoint, region, timeToLiveMillis, cacheSize, avroRecordType, avroReaderSchema,
        protobufMessageType, registryName, compatibilitySetting, description,
        isSchemaAutoRegistrationEnabled, isJsonClassNameResolutionEnabled, isJsonSchemaNullableEnabled,
        isJsonSchemaCompatibilityCheckEnabled, jsonClassNameAllowlist,
        tags, metadata, secondaryDeserializer, proxyUrl, userAgentApp,
        jacksonSerializationFeatures, jacksonDeserializationFeatures,
        jacksonSerializationFeatureToggles, jacksonDeserializationFeatureToggles,
    ).fold(1) { acc, value -> 31 * acc + (value?.hashCode() ?: 0) }

    override fun toString(): String = "GlueSchemaRegistryConfiguration(compressionType=$compressionType, endPoint=$endPoint, " +
        "region=$region, timeToLiveMillis=$timeToLiveMillis, cacheSize=$cacheSize, " +
        "avroRecordType=$avroRecordType, avroReaderSchema=$avroReaderSchema, " +
        "protobufMessageType=$protobufMessageType, " +
        "registryName=$registryName, compatibilitySetting=$compatibilitySetting, " +
        "description=$description, schemaAutoRegistrationEnabled=$isSchemaAutoRegistrationEnabled, " +
        "jsonClassNameResolutionEnabled=$isJsonClassNameResolutionEnabled, " +
        "jsonSchemaNullableEnabled=$isJsonSchemaNullableEnabled, " +
        "jsonSchemaCompatibilityCheckEnabled=$isJsonSchemaCompatibilityCheckEnabled, " +
        "jsonClassNameAllowlist=$jsonClassNameAllowlist, tags=$tags, metadata=$metadata, " +
        "secondaryDeserializer=$secondaryDeserializer, proxyUrl=$proxyUrl, userAgentApp=$userAgentApp, " +
        "jacksonSerializationFeatures=$jacksonSerializationFeatures, " +
        "jacksonDeserializationFeatures=$jacksonDeserializationFeatures, " +
        "jacksonSerializationFeatureToggles=$jacksonSerializationFeatureToggles, " +
        "jacksonDeserializationFeatureToggles=$jacksonDeserializationFeatureToggles)"

    protected fun getMapFromPropertiesFile(properties: Properties): Map<String, *> = HashMap(properties.entries.associate { it.key.toString() to it.value })

    private fun buildDescriptionFromProperties(): String = "DEFAULT-DESCRIPTION$DELIMITER$region$DELIMITER$registryName"

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryConfiguration::class.java)
        private const val DELIMITER = "-"

        /**
         * Suffix marking an allowlist entry as a package rather than a class, as in
         * `"com.example.pojos.*"`.
         */
        private const val PACKAGE_WILDCARD_SUFFIX = ".*"

        /**
         * Whether an allowlist entry is a wildcard with no package to scope it.
         */
        private fun isBareWildcard(entry: String): Boolean = entry == "*" || entry == PACKAGE_WILDCARD_SUFFIX

        /**
         * Whether [className] sits directly in the package named by a `".*"` allowlist entry,
         * with no further package segment.
         */
        private fun isDirectlyInPackage(
            className: String,
            packageEntry: String,
        ): Boolean {
            // Drop only the "*", keeping the dot before it, so that "com.example.pojos.*" cannot
            // match "com.example.pojosX".
            val packagePrefix = packageEntry.substring(0, packageEntry.length - 1)
            if (!className.startsWith(packagePrefix)) {
                return false
            }
            val remainder = className.substring(packagePrefix.length)
            return remainder.isNotEmpty() && remainder.indexOf('.') < 0
        }
    }
}
