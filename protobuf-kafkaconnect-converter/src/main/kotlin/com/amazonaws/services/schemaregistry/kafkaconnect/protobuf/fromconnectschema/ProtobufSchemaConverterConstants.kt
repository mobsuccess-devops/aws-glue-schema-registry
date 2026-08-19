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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema

object ProtobufSchemaConverterConstants {
    /**
     * Kafka Converters / connectors can define this property in the Connect schema to specify the
     * Protobuf tag number to use for a field while generating the Protobuf schema.
     */
    const val PROTOBUF_TAG = "protobuf.tag"

    /**
     * Kafka Converters / connectors can define this property in the Connect schema to specify the
     * Protobuf type to use for a field while generating the Protobuf schema. For example int32 can
     * be mapped to sint32 or uint32; it defaults to int32 when unspecified.
     */
    const val PROTOBUF_TYPE = "protobuf.type"

    /**
     * Specifies the package name of the Protobuf schema definition, available in the parent level
     * connect schema parameters.
     */
    const val PROTOBUF_PACKAGE = "protobuf.package"

    /** Metadata parameter holding the name of the enum. */
    const val PROTOBUF_ENUM_NAME = "ENUM_NAME"

    /** Marks metadata parameters holding values for the enum. */
    const val PROTOBUF_ENUM_VALUE = "PROTOBUF_ENUM_VALUE."

    /** Validates that the protobuf type is an enum. */
    const val PROTOBUF_ENUM_TYPE = "enum"

    /** Validates that the protobuf type is a oneof. */
    const val PROTOBUF_ONEOF_TYPE = "oneof"

    /**
     * Kafka Connect's Decimal builder requires a default scale. The converter overrides this value
     * during conversion, so it is only a temporary default at creation time.
     */
    const val DECIMAL_DEFAULT_SCALE = 0

    /** Specifies the decimal scale value during conversion. */
    const val DECIMAL_SCALE_VALUE = "connect.decimal.scale"

    /** Specifies the Connect schema type, preserving consistency during conversion. */
    const val CONNECT_SCHEMA_TYPE = "connect.schema"

    /** Specifies the Connect schema type as int8. */
    const val CONNECT_SCHEMA_INT8 = "int8"

    /** Specifies the Connect schema type as int16. */
    const val CONNECT_SCHEMA_INT16 = "int16"

    /** Import statement for the Metadata type. */
    const val METADATA_IMPORT = "metadata/metadata.proto"
}
