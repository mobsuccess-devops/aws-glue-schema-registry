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

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT16
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT8
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.METADATA_IMPORT
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.google.common.collect.ImmutableMap
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64
import metadata.ProtobufSchemaMetadata
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException

/**
 * Converts Primitive Connect schema types to Protobuf primitive types.
 */
class PrimitiveSchemaTypeConverter : SchemaTypeConverter {
    private fun getProtobufType(schema: Schema): DescriptorProtos.FieldDescriptorProto.Type {
        val schemaType = schema.type()
        if (!CONNECT_PROTO_CONVERSION_MAP.containsKey(schemaType)) {
            throw IllegalStateException("Invalid connect type passed to Primitive type converter: $schemaType")
        }

        val type = CONNECT_PROTO_CONVERSION_MAP[schemaType]!!
        val schemaParams = schema.parameters()

        if (schemaParams == null ||
            !schemaParams.containsKey(PROTOBUF_TYPE) ||
            !CONNECT_METADATA_TYPE_CONVERSION_MAP.containsKey(type)
        ) {
            return type
        }

        // Map to any valid protobuf type specified in the metadata.
        val specifiedProtobufType =
            DescriptorProtos.FieldDescriptorProto.Type.valueOf(
                "TYPE_" + schemaParams[PROTOBUF_TYPE]!!.uppercase(),
            )

        if (!CONNECT_METADATA_TYPE_CONVERSION_MAP[type]!!.contains(specifiedProtobufType)) {
            throw DataException(
                "Protobuf type for $type is specified to use $specifiedProtobufType which is not allowed",
            )
        }

        return specifiedProtobufType
    }

    private fun setMetadataOptions(
        builder: DescriptorProtos.FieldDescriptorProto.Builder,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
        metadataKey: String,
        metadataValue: String,
    ) {
        addImportToProtobufSchema(fileDescriptorProtoBuilder, METADATA_IMPORT)

        val keyOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
        keyOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, metadataKey)
        builder.mergeOptions(keyOptionsBuilder.build())

        val valueOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
        valueOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataValue, metadataValue)
        builder.mergeOptions(valueOptionsBuilder.build())
    }

    override fun toProtobufSchema(
        schema: Schema,
        descriptorProto: DescriptorProtos.DescriptorProto.Builder,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
    ): DescriptorProtos.FieldDescriptorProto.Builder {
        val builder =
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(getProtobufType(schema))
                // Label is OPTIONAL as this is a simple primitive type.
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)

        if (schema.type() == Schema.Type.INT8) {
            setMetadataOptions(builder, fileDescriptorProtoBuilder, CONNECT_SCHEMA_TYPE, CONNECT_SCHEMA_INT8)
        } else if (schema.type() == Schema.Type.INT16) {
            setMetadataOptions(builder, fileDescriptorProtoBuilder, CONNECT_SCHEMA_TYPE, CONNECT_SCHEMA_INT16)
        }

        return builder
    }

    companion object {
        private val CONNECT_PROTO_CONVERSION_MAP: Map<Schema.Type, DescriptorProtos.FieldDescriptorProto.Type> =
            ImmutableMap
                .builder<Schema.Type, DescriptorProtos.FieldDescriptorProto.Type>()
                .put(Schema.Type.INT8, TYPE_INT32)
                .put(Schema.Type.INT16, TYPE_INT32)
                .put(Schema.Type.INT32, TYPE_INT32)
                .put(Schema.Type.INT64, TYPE_INT64)
                .put(Schema.Type.FLOAT32, TYPE_FLOAT)
                .put(Schema.Type.FLOAT64, TYPE_DOUBLE)
                .put(Schema.Type.BOOLEAN, TYPE_BOOL)
                .put(Schema.Type.BYTES, TYPE_BYTES)
                .put(Schema.Type.STRING, TYPE_STRING)
                .build()

        private val CONNECT_METADATA_TYPE_CONVERSION_MAP:
            Map<DescriptorProtos.FieldDescriptorProto.Type, List<DescriptorProtos.FieldDescriptorProto.Type>> =
            ImmutableMap.of(
                TYPE_INT32,
                listOf(TYPE_SINT32, TYPE_SFIXED32, TYPE_INT32),
                TYPE_INT64,
                listOf(TYPE_SINT64, TYPE_UINT64, TYPE_SFIXED64, TYPE_FIXED64, TYPE_FIXED32, TYPE_UINT32, TYPE_INT64),
            )
    }
}
