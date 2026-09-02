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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat
import java.net.URI

/**
 * Marks the receiver of a Glue Schema Registry configuration block, so that a nested block
 * cannot reach the properties of an enclosing one by accident.
 */
@DslMarker
public annotation class GlueSchemaRegistryDsl

/**
 * Builds the property map the serializers, deserializers and converters read, with a typed
 * property per configuration key.
 *
 * Only the properties actually set appear in [build]: a key left alone is absent from the map,
 * so the library applies its own default rather than one this builder invented.
 */
@GlueSchemaRegistryDsl
public class GlueSchemaRegistryConfigBuilder internal constructor() {
    private val properties = LinkedHashMap<String, Any>()

    /** AWS region of the registry. Resolved through the default provider chain when unset. */
    public var region: String?
        get() = properties[AWSSchemaRegistryConstants.AWS_REGION] as String?
        set(value) = put(AWSSchemaRegistryConstants.AWS_REGION, value)

    /** Endpoint override for the AWS Glue service. */
    public var endpoint: String?
        get() = properties[AWSSchemaRegistryConstants.AWS_ENDPOINT] as String?
        set(value) = put(AWSSchemaRegistryConstants.AWS_ENDPOINT, value)

    /** HTTP proxy the Glue client routes its calls through. */
    public var proxyUrl: URI?
        get() = (properties[AWSSchemaRegistryConstants.PROXY_URL] as String?)?.let(URI::create)
        set(value) = put(AWSSchemaRegistryConstants.PROXY_URL, value?.toString())

    /** Name of the registry holding the schemas. Defaults to `default-registry`. */
    public var registryName: String?
        get() = properties[AWSSchemaRegistryConstants.REGISTRY_NAME] as String?
        set(value) = put(AWSSchemaRegistryConstants.REGISTRY_NAME, value)

    /** Schema name. Derived from the topic by the naming strategy when unset. */
    public var schemaName: String?
        get() = properties[AWSSchemaRegistryConstants.SCHEMA_NAME] as String?
        set(value) = put(AWSSchemaRegistryConstants.SCHEMA_NAME, value)

    /** Fully qualified name of an `AWSSchemaNamingStrategy` implementation. */
    public var schemaNamingStrategyClass: String?
        get() = properties[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] as String?
        set(value) = put(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS, value)

    /** Whether a schema version absent from the registry is registered automatically. */
    public var autoRegistration: Boolean?
        get() = properties[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] as Boolean?
        set(value) = put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, value)

    /** Compatibility mode applied to a schema this producer creates. */
    public var compatibility: Compatibility?
        get() = (properties[AWSSchemaRegistryConstants.COMPATIBILITY_SETTING] as String?)?.let(Compatibility::fromValue)
        set(value) = put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, value?.toString())

    /** Description attached to a schema this producer registers. */
    public var description: String?
        get() = properties[AWSSchemaRegistryConstants.DESCRIPTION] as String?
        set(value) = put(AWSSchemaRegistryConstants.DESCRIPTION, value)

    /** Compression applied to the serialized payload. */
    public var compression: AWSSchemaRegistryConstants.COMPRESSION?
        get() = (properties[AWSSchemaRegistryConstants.COMPRESSION_TYPE] as String?)
            ?.let(AWSSchemaRegistryConstants.COMPRESSION::valueOf)
        set(value) = put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, value?.name)

    /** Data format read and written by the format-agnostic serializer. */
    public var dataFormat: DataFormat?
        get() = (properties[AWSSchemaRegistryConstants.DATA_FORMAT] as String?)?.let(DataFormat::fromValue)
        set(value) = put(AWSSchemaRegistryConstants.DATA_FORMAT, value?.toString())

    /** Avro record representation handed to the application. */
    public var avroRecordType: AvroRecordType?
        get() = (properties[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] as String?)?.let(AvroRecordType::valueOf)
        set(value) = put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, value?.name)

    /** Protobuf message representation handed to the application. */
    public var protobufMessageType: ProtobufMessageType?
        get() = (properties[AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE] as String?)
            ?.let(ProtobufMessageType::valueOf)
        set(value) = put(AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE, value?.name)

    /** Deserializer to fall back to for records carrying no Glue Schema Registry header. */
    public var secondaryDeserializer: String?
        get() = properties[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER] as String?
        set(value) = put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, value)

    /** Application name reported in the User-Agent of the Glue calls. */
    public var userAgentApp: String?
        get() = properties[AWSSchemaRegistryConstants.USER_AGENT_APP] as String?
        set(value) = put(AWSSchemaRegistryConstants.USER_AGENT_APP, value)

    /** Maximum number of schemas held in the caches. */
    public var cacheSize: Int?
        get() = (properties[AWSSchemaRegistryConstants.CACHE_SIZE] as String?)?.toInt()
        set(value) = put(AWSSchemaRegistryConstants.CACHE_SIZE, value?.toString())

    /** Time to live, in milliseconds, of a cache entry. */
    public var cacheTimeToLiveMillis: Long?
        get() = (properties[AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS] as String?)?.toLong()
        set(value) = put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, value?.toString())

    /** Whether the JSON deserializer may instantiate the POJO named by a schema. */
    public var jsonClassNameResolutionEnabled: Boolean?
        get() = properties[AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED] as Boolean?
        set(value) = put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, value)

    /** Whether a JSON schema derived from a POJO offers `null` alongside an optional field's type. */
    public var jsonSchemaNullableEnabled: Boolean?
        get() = properties[AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED] as Boolean?
        set(value) = put(AWSSchemaRegistryConstants.JSON_SCHEMA_NULLABLE_ENABLED, value)

    /** Whether a new JSON schema version is compared against the latest one before registration. */
    public var jsonSchemaCompatibilityCheckEnabled: Boolean?
        get() = properties[AWSSchemaRegistryConstants.JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED] as Boolean?
        set(value) = put(AWSSchemaRegistryConstants.JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED, value)

    /** Classes the JSON deserializer may instantiate. An entry ending in `.*` scopes a package. */
    public fun jsonClassNameAllowlist(vararg classNames: String) {
        put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, classNames.toList())
    }

    /** Classes the JSON deserializer may instantiate, given by type. */
    public fun jsonClassNameAllowlist(vararg classes: Class<*>) {
        put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, classes.map { it.name })
    }

    /** Jackson serialization features to enable. */
    public fun jacksonSerializationFeatures(vararg features: SerializationFeature) {
        put(AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES, features.map { it.name })
    }

    /** Jackson deserialization features to enable. */
    public fun jacksonDeserializationFeatures(vararg features: DeserializationFeature) {
        put(AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES, features.map { it.name })
    }

    /** Jackson serialization features to enable or disable, one entry per feature. */
    public fun jacksonSerializationFeatures(features: Map<SerializationFeature, Boolean>) {
        put(
            AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES,
            features.entries.associate { (feature, enabled) -> feature.name to enabled },
        )
    }

    /** Jackson deserialization features to enable or disable, one entry per feature. */
    public fun jacksonDeserializationFeatures(features: Map<DeserializationFeature, Boolean>) {
        put(
            AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES,
            features.entries.associate { (feature, enabled) -> feature.name to enabled },
        )
    }

    /**
     * Tags applied to the registry entry when this producer creates it.
     *
     * The map is copied into a [HashMap], which is the one shape
     * `GlueSchemaRegistryConfiguration` accepts for this key.
     */
    public fun tags(tags: Map<String, String>) {
        put(AWSSchemaRegistryConstants.TAGS, HashMap(tags))
    }

    /** Metadata attached to the schema version. Copied into a [HashMap], as [tags] is. */
    public fun metadata(metadata: Map<String, String>) {
        put(AWSSchemaRegistryConstants.METADATA, HashMap(metadata))
    }

    /** Sets a property this builder has no typed accessor for. */
    public fun property(
        key: String,
        value: Any?,
    ) {
        put(key, value)
    }

    /** The properties set on this builder, in the order they were set. */
    public fun build(): Map<String, Any> = LinkedHashMap(properties)

    private fun put(
        key: String,
        value: Any?,
    ) {
        if (value == null) {
            properties.remove(key)
        } else {
            properties[key] = value
        }
    }
}
