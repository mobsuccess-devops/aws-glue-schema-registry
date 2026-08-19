/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

// `open`: the test suites mock this type.
open class AvroDataConfig(
    props: Map<*, *>,
) : AbstractConfig(baseConfigDef(), props) {
    open fun isEnhancedAvroSchemaSupport(): Boolean = getBoolean(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG)

    open fun isConnectMetaData(): Boolean = getBoolean(CONNECT_META_DATA_CONFIG)

    open fun getSchemasCacheSize(): Int = getInt(SCHEMAS_CACHE_SIZE_CONFIG)

    class Builder {
        private val props: MutableMap<String, Any> = HashMap()

        fun with(
            key: String,
            value: Any,
        ): Builder = apply { props[key] = value }

        fun build(): AvroDataConfig = AvroDataConfig(props)
    }

    companion object {
        const val ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG = "enhanced.avro.schema.support"
        const val ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = false
        const val ENHANCED_AVRO_SCHEMA_SUPPORT_DOC =
            "Toggle for enabling/disabling enhanced avro schema support: Enum symbol preservation and " +
                "Package Name awareness"

        const val CONNECT_META_DATA_CONFIG = "connect.meta.data"
        const val CONNECT_META_DATA_DEFAULT = true
        const val CONNECT_META_DATA_DOC =
            "Toggle for enabling/disabling connect converter to add its meta data to the output schema or not"

        const val SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config"
        const val SCHEMAS_CACHE_SIZE_DEFAULT = 1000
        const val SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache"

        @JvmStatic
        fun baseConfigDef(): ConfigDef = ConfigDef()
            .define(
                ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                ENHANCED_AVRO_SCHEMA_SUPPORT_DOC,
            ).define(
                CONNECT_META_DATA_CONFIG,
                ConfigDef.Type.BOOLEAN,
                CONNECT_META_DATA_DEFAULT,
                ConfigDef.Importance.LOW,
                CONNECT_META_DATA_DOC,
            ).define(
                SCHEMAS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                SCHEMAS_CACHE_SIZE_DEFAULT,
                ConfigDef.Importance.LOW,
                SCHEMAS_CACHE_SIZE_DOC,
            )
    }
}
