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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import software.amazon.awssdk.services.glue.model.Compatibility
import software.amazon.awssdk.services.glue.model.DataFormat

/**
 * The Glue Schema Registry configuration keys, described as a Kafka [ConfigDef].
 *
 * The Kafka Connect converters of this project build their own `ConfigDef` from these
 * definitions, which is what makes `PUT /connector-plugins/{plugin}/config/validate` report the
 * registry settings, and what lets a Connect UI render them.
 *
 * The definitions describe the keys that
 * [com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration] reads;
 * their types, defaults and accepted values are those that class enforces.
 */
public object GlueSchemaRegistryConfigDef {
    private const val PACKAGE_WILDCARD = ".*"

    public const val GROUP_AWS: String = "AWS"
    public const val GROUP_REGISTRY: String = "Schema registry"
    public const val GROUP_SERIALIZATION: String = "Serialization"
    public const val GROUP_CACHE: String = "Cache"
    public const val GROUP_AVRO: String = "Avro"
    public const val GROUP_JSON: String = "JSON Schema"
    public const val GROUP_PROTOBUF: String = "Protobuf"

    /**
     * The keys understood by every Glue Schema Registry Connect converter.
     */
    @JvmStatic
    public fun baseConfigDef(): ConfigDef = ConfigDef()
        .define(
            AWSSchemaRegistryConstants.AWS_REGION,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "AWS region of the Glue Schema Registry to talk to. When unset, the region is resolved " +
                "through the default AWS region provider chain, and configuration fails if that chain " +
                "resolves nothing.",
            GROUP_AWS,
            1,
            ConfigDef.Width.MEDIUM,
            "AWS region",
        ).define(
            AWSSchemaRegistryConstants.AWS_ENDPOINT,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "Endpoint override for the AWS Glue service. Leave unset to use the regional endpoint.",
            GROUP_AWS,
            2,
            ConfigDef.Width.LONG,
            "AWS endpoint",
        ).define(
            AWSSchemaRegistryConstants.PROXY_URL,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "URL of the HTTP proxy the Glue client routes its calls through, for example " +
                "http://proxy.example.com:8080.",
            GROUP_AWS,
            3,
            ConfigDef.Width.LONG,
            "Proxy URL",
        ).define(
            AWSSchemaRegistryConstants.REGISTRY_NAME,
            ConfigDef.Type.STRING,
            AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME,
            ConfigDef.Importance.MEDIUM,
            "Name of the registry that holds the schemas.",
            GROUP_REGISTRY,
            1,
            ConfigDef.Width.MEDIUM,
            "Registry name",
        ).define(
            AWSSchemaRegistryConstants.SCHEMA_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.MEDIUM,
            "Name of the schema to read and write. When unset, the name is produced by the schema " +
                "naming strategy, whose default derives it from the topic name.",
            GROUP_REGISTRY,
            2,
            ConfigDef.Width.MEDIUM,
            "Schema name",
        ).define(
            AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "Fully qualified name of a class implementing AWSSchemaNamingStrategy, used to derive the " +
                "schema name when " + AWSSchemaRegistryConstants.SCHEMA_NAME + " is unset.",
            GROUP_REGISTRY,
            3,
            ConfigDef.Width.LONG,
            "Schema naming strategy class",
        ).define(
            AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            "Whether a schema version absent from the registry is registered automatically. When false, " +
                "serialization of an unknown schema fails instead.",
            GROUP_REGISTRY,
            4,
            ConfigDef.Width.SHORT,
            "Auto-register schemas",
        ).define(
            AWSSchemaRegistryConstants.COMPATIBILITY_SETTING,
            ConfigDef.Type.STRING,
            AWSSchemaRegistryConstants.DEFAULT_COMPATIBILITY_SETTING.toString(),
            optionalValidString(compatibilityValues(), caseSensitive = false),
            ConfigDef.Importance.MEDIUM,
            "Compatibility mode applied to a schema this converter creates. Only read when " +
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING + " is true.",
            GROUP_REGISTRY,
            5,
            ConfigDef.Width.MEDIUM,
            "Compatibility",
        ).define(
            AWSSchemaRegistryConstants.DESCRIPTION,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "Description attached to a schema this converter registers. Defaults to " +
                "DEFAULT-DESCRIPTION-<region>-<registry name>.",
            GROUP_REGISTRY,
            6,
            ConfigDef.Width.LONG,
            "Schema description",
        ).define(
            AWSSchemaRegistryConstants.COMPRESSION_TYPE,
            ConfigDef.Type.STRING,
            AWSSchemaRegistryConstants.COMPRESSION.NONE.name,
            optionalValidString(
                AWSSchemaRegistryConstants.COMPRESSION.entries.map { it.name },
                caseSensitive = false,
            ),
            ConfigDef.Importance.MEDIUM,
            "Compression applied to the serialized payload. ZLIB trades CPU for smaller records; a " +
                "consumer reads either, since the compression is recorded in the record header.",
            GROUP_SERIALIZATION,
            1,
            ConfigDef.Width.SHORT,
            "Compression",
        ).define(
            AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            "Fully qualified name of a Deserializer to fall back to for records that carry no Glue " +
                "Schema Registry header, which is what makes a migration from another registry readable.",
            GROUP_SERIALIZATION,
            2,
            ConfigDef.Width.LONG,
            "Secondary deserializer",
        ).define(
            AWSSchemaRegistryConstants.CACHE_SIZE,
            ConfigDef.Type.INT,
            AWSSchemaRegistryConstants.DEFAULT_CACHE_SIZE,
            ConfigDef.Importance.LOW,
            "Maximum number of schemas held in the serializer and deserializer caches.",
            GROUP_CACHE,
            1,
            ConfigDef.Width.SHORT,
            "Cache size",
        ).define(
            AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS,
            ConfigDef.Type.LONG,
            AWSSchemaRegistryConstants.DEFAULT_CACHE_TIME_TO_LIVE_MILLIS,
            ConfigDef.Importance.LOW,
            "Time to live, in milliseconds, of an entry of the serializer and deserializer caches.",
            GROUP_CACHE,
            2,
            ConfigDef.Width.MEDIUM,
            "Cache time to live",
        )

    /**
     * Adds the data format key, fixed to [dataFormat], which is the one format a converter reads
     * and writes.
     */
    @JvmStatic
    public fun defineDataFormat(
        configDef: ConfigDef,
        dataFormat: DataFormat,
    ): ConfigDef = configDef.define(
        AWSSchemaRegistryConstants.DATA_FORMAT,
        ConfigDef.Type.STRING,
        dataFormat.toString(),
        optionalValidString(listOf(dataFormat.toString()), caseSensitive = false),
        ConfigDef.Importance.HIGH,
        "Data format of the records this converter reads and writes. A producer has to set it; a " +
            "consumer reads the format from the record header and may leave it unset.",
        GROUP_SERIALIZATION,
        4,
        ConfigDef.Width.SHORT,
        "Data format",
    )

    /**
     * Adds the keys of the Avro serializer and deserializer.
     */
    @JvmStatic
    public fun defineAvro(configDef: ConfigDef): ConfigDef = configDef.define(
        AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
        ConfigDef.Type.STRING,
        AvroRecordType.GENERIC_RECORD.name,
        optionalValidString(AvroRecordType.entries.map { it.name }, caseSensitive = true),
        ConfigDef.Importance.MEDIUM,
        "Avro record representation handed to Connect: GENERIC_RECORD reads any schema, " +
            "SPECIFIC_RECORD requires the generated classes on the worker classpath.",
        GROUP_AVRO,
        1,
        ConfigDef.Width.MEDIUM,
        "Avro record type",
    )

    /**
     * Adds the keys of the Protobuf serializer and deserializer.
     */
    @JvmStatic
    public fun defineProtobuf(configDef: ConfigDef): ConfigDef = configDef.define(
        AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE,
        ConfigDef.Type.STRING,
        null,
        optionalValidString(ProtobufMessageType.entries.map { it.name }, caseSensitive = true),
        ConfigDef.Importance.MEDIUM,
        "Protobuf message representation handed to Connect: DYNAMIC_MESSAGE reads any schema, " +
            "POJO requires the generated classes on the worker classpath.",
        GROUP_PROTOBUF,
        1,
        ConfigDef.Width.MEDIUM,
        "Protobuf message type",
    )

    /**
     * Adds the keys of the JSON Schema serializer and deserializer.
     */
    @JvmStatic
    public fun defineJson(configDef: ConfigDef): ConfigDef = configDef
        .define(
            AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES,
            ConfigDef.Type.LIST,
            null,
            enumNameList(SerializationFeature.entries.map { it.name }),
            ConfigDef.Importance.LOW,
            "Names of com.fasterxml.jackson.databind.SerializationFeature entries to enable.",
            GROUP_JSON,
            1,
            ConfigDef.Width.LONG,
            "Jackson serialization features",
        ).define(
            AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES,
            ConfigDef.Type.LIST,
            null,
            enumNameList(DeserializationFeature.entries.map { it.name }),
            ConfigDef.Importance.LOW,
            "Names of com.fasterxml.jackson.databind.DeserializationFeature entries to enable.",
            GROUP_JSON,
            2,
            ConfigDef.Width.LONG,
            "Jackson deserialization features",
        ).define(
            AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.MEDIUM,
            "Whether the deserializer may instantiate the POJO named by the className field of a " +
                "schema. Off by default: it turns a registry entry into a class name to load.",
            GROUP_JSON,
            3,
            ConfigDef.Width.SHORT,
            "Resolve class names",
        ).define(
            AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST,
            ConfigDef.Type.LIST,
            null,
            NoBareWildcard,
            ConfigDef.Importance.MEDIUM,
            "Classes the deserializer may instantiate when " +
                AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED + " is true. An entry " +
                "matches exactly, or scopes one package when it ends in .* — a bare wildcard is rejected.",
            GROUP_JSON,
            4,
            ConfigDef.Width.LONG,
            "Class name allowlist",
        )

    /**
     * Renders the values of [props] the way the converters have to hand them on: a value of a
     * [ConfigDef.Type.STRING] key through `toString()`, or `Class.getName()` for a `Class`, and a
     * comma-separated value of a [ConfigDef.Type.LIST] key as the list it denotes. Every other
     * entry is returned unchanged, as is [props] itself when nothing needs rendering.
     */
    @JvmStatic
    public fun coerce(
        configDef: ConfigDef,
        props: Map<String, *>,
    ): Map<String, *> {
        val configKeys = configDef.configKeys()
        val coerced = LinkedHashMap<String, Any?>(props.size)
        var changed = false
        props.forEach { (key, value) ->
            val rendered = render(configKeys[key]?.type, value)
            changed = changed || rendered !== value
            coerced[key] = rendered
        }
        return if (changed) coerced else props
    }

    private fun render(
        type: ConfigDef.Type?,
        value: Any?,
    ): Any? = when {
        value == null -> value
        type == ConfigDef.Type.STRING && value !is String -> asConfigString(value)
        type == ConfigDef.Type.LIST && value is String -> value.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        else -> value
    }

    private fun asConfigString(value: Any): String = if (value is Class<*>) value.name else value.toString()

    private fun compatibilityValues(): List<String> = Compatibility
        .knownValues()
        .map { it.toString() }
        .sorted()

    private fun dataFormatValues(): List<String> = DataFormat
        .knownValues()
        .map { it.toString() }
        .sorted()

    private fun enumNameList(validValues: List<String>): ConfigDef.Validator = EnumNameList(validValues)

    private object NoBareWildcard : ConfigDef.Validator {
        override fun ensureValid(
            name: String,
            value: Any?,
        ) {
            val entries = value as? List<*> ?: return
            if (entries.any { it == "*" || it == PACKAGE_WILDCARD }) {
                throw ConfigException(
                    name,
                    value,
                    "A bare wildcard allows every class on the classpath. List classes explicitly, or " +
                        "scope a package with a prefix such as \"com.example.pojos.*\".",
                )
            }
        }

        override fun toString(): String = "Class names, or package prefixes ending in $PACKAGE_WILDCARD"
    }

    private class EnumNameList(
        private val validValues: List<String>,
    ) : ConfigDef.Validator {
        override fun ensureValid(
            name: String,
            value: Any?,
        ) {
            val entries = value as? List<*> ?: return
            val unknown = entries.map { it.toString() }.filter { it !in validValues }
            if (unknown.isNotEmpty()) {
                throw ConfigException(
                    name,
                    value,
                    "Unknown ${if (unknown.size == 1) "entry" else "entries"} ${unknown.joinToString(", ")}. " +
                        "Entries must be among: ${validValues.joinToString(", ")}",
                )
            }
        }

        override fun toString(): String = "[${validValues.joinToString(", ")}]"
    }

    private fun optionalValidString(
        validValues: List<String>,
        caseSensitive: Boolean,
    ): ConfigDef.Validator = OptionalValidString(validValues, caseSensitive)

    private class OptionalValidString(
        private val validValues: List<String>,
        private val caseSensitive: Boolean,
    ) : ConfigDef.Validator {
        override fun ensureValid(
            name: String,
            value: Any?,
        ) {
            if (value == null) {
                return
            }
            val candidate = value as String
            val accepted =
                if (caseSensitive) {
                    validValues.contains(candidate)
                } else {
                    validValues.any { it.equals(candidate, ignoreCase = true) }
                }
            if (!accepted) {
                throw ConfigException(name, value, "String must be one of: ${validValues.joinToString(", ")}")
            }
        }

        override fun toString(): String = "[${validValues.joinToString(", ")}]"
    }
}
