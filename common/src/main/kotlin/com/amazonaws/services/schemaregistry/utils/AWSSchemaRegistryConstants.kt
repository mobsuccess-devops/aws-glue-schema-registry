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

package com.amazonaws.services.schemaregistry.utils

import software.amazon.awssdk.services.glue.model.Compatibility

object AWSSchemaRegistryConstants {
    const val PROXY_URL = "proxyUrl"
    const val AWS_ENDPOINT = "endpoint"
    const val AWS_REGION = "region"
    const val HEADER_VERSION_BYTE: Byte = 3
    const val COMPRESSION_TYPE = "compression"
    const val COMPRESSION_BYTE: Byte = 5
    const val COMPRESSION_DEFAULT_BYTE: Byte = 0
    const val HEADER_VERSION_BYTE_SIZE = 1
    const val COMPRESSION_BYTE_SIZE = 1
    const val SCHEMA_VERSION_ID_SIZE = 16
    const val SCHEMA_NAME = "schemaName"
    const val SCHEMA_NAMING_GENERATION_CLASS = "schemaNameGenerationClass"
    const val DEFAULT_SCHEMA_STRATEGY = "AWSSchemaNamingStrategyDefaultImpl"
    const val DATA_FORMAT = "dataFormat"
    const val CACHE_TIME_TO_LIVE_MILLIS = "timeToLiveMillis"
    const val DEFAULT_CACHE_TIME_TO_LIVE_MILLIS = 24 * 60 * 60 * 1000L
    const val CACHE_SIZE = "cacheSize"
    const val DEFAULT_CACHE_SIZE = 200
    const val AVRO_RECORD_TYPE = "avroRecordType"
    const val AVRO_READER_SCHEMA = "avroReaderSchema"
    const val PROTOBUF_MESSAGE_TYPE = "protobufMessageType"
    const val REGISTRY_NAME = "registry.name"
    const val DEFAULT_REGISTRY_NAME = "default-registry"
    const val COMPATIBILITY_SETTING = "compatibility"
    const val DESCRIPTION = "description"

    // Not a primitive: @JvmField keeps it a static field on the Java side.
    @JvmField
    val DEFAULT_COMPATIBILITY_SETTING: Compatibility = Compatibility.BACKWARD

    const val SECONDARY_DESERIALIZER = "secondaryDeserializer"
    const val SCHEMA_VERSION_NOT_FOUND_MSG = "Schema version is not found."
    const val TAGS_CONFIG_NOT_HASHMAP_MSG = "The tag config is not a instance of HashMap."
    const val SCHEMA_NOT_FOUND_MSG = "Schema is not found."
    const val AUTO_REGISTRATION_IS_DISABLED_MSG =
        "Failed to auto-register schema. Auto registration of schema is not enabled."
    const val SCHEMA_AUTO_REGISTRATION_SETTING = "schemaAutoRegistrationEnabled"
    const val TAGS = "tags"
    const val METADATA = "metadata"
    const val TRANSPORT_METADATA_KEY = "x-amz-meta-transport"
    const val JACKSON_SERIALIZATION_FEATURES = "jacksonSerializationFeatures"
    const val JACKSON_DESERIALIZATION_FEATURES = "jacksonDeserializationFeatures"
    const val USER_AGENT_APP = "userAgentApp"
    const val JSON_SCHEMA_NULLABLE_ENABLED = "jsonSchemaNullableEnabled"
    const val JSON_SCHEMA_COMPATIBILITY_CHECK_ENABLED = "jsonSchemaCompatibilityCheckEnabled"
    const val JSON_CLASS_NAME_RESOLUTION_ENABLED = "jsonClassNameResolutionEnabled"
    const val JSON_CLASS_NAME_ALLOWLIST = "jsonClassNameAllowlist"
    const val ASSUME_ROLE_ARN = "assumeRoleArn"
    const val ASSUME_ROLE_SESSION_NAME = "assumeRoleSessionName"

    enum class SchemaVersionStatus {
        AVAILABLE,
        PENDING,
        FAILURE,
        DELETING,
    }

    enum class COMPRESSION {
        NONE,
        ZLIB,
    }
}
