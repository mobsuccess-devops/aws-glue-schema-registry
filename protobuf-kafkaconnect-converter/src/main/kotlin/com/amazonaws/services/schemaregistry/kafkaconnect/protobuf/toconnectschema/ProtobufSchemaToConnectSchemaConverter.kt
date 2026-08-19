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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT16
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_INT8
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.CONNECT_SCHEMA_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_SCALE_VALUE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_PACKAGE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.google.common.collect.ImmutableSet
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM
import com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32
import com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED64
import com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED32
import com.google.protobuf.Descriptors.FieldDescriptor.Type.SFIXED64
import com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT32
import com.google.protobuf.Descriptors.FieldDescriptor.Type.SINT64
import com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32
import com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT64
import com.google.protobuf.Message
import metadata.ProtobufSchemaMetadata
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException

/**
 * Converts the Protobuf schema to Connect schemas.
 * Partially inspired from the blueapron kafka-connect-protobuf-converter.
 */
class ProtobufSchemaToConnectSchemaConverter {
    fun toConnectSchema(message: Message): Schema = toConnectSchema(message, HashSet())

    private fun toConnectSchema(
        message: Message,
        visitedTypes: MutableSet<String>,
    ): Schema {
        val builder = SchemaBuilder.struct()
        val descriptor = message.descriptorForType

        builder.name(descriptor.name)
        builder.version(CONVERTER_VERSION)
        builder.parameter(PROTOBUF_PACKAGE, descriptor.file.getPackage())

        for (fieldDescriptor in descriptor.fields) {
            val oneofDescriptor = fieldDescriptor.realContainingOneof
            if (oneofDescriptor != null) {
                if (builder.fields().none { it.name() == oneofDescriptor.name }) {
                    builder.field(oneofDescriptor.name, toConnectSchemaForOneOfField(oneofDescriptor, visitedTypes))
                }
                continue
            }
            builder.field(fieldDescriptor.name, toConnectSchemaForField(fieldDescriptor, visitedTypes))
        }

        return builder.build()
    }

    private fun toConnectSchemaForField(
        fieldDescriptor: Descriptors.FieldDescriptor,
        visitedTypes: MutableSet<String>,
    ): Schema = toConnectSchemaBuilderForField(fieldDescriptor, visitedTypes).build()

    private fun toConnectSchemaForOneOfField(
        oneofDescriptor: Descriptors.OneofDescriptor,
        visitedTypes: MutableSet<String>,
    ): Schema {
        val builder = SchemaBuilder.struct().name(oneofDescriptor.name)
        for (fieldDescriptor in oneofDescriptor.fields) {
            builder.field(
                fieldDescriptor.name,
                toConnectSchemaBuilderForField(fieldDescriptor, visitedTypes).optional().build(),
            )
        }
        builder.parameter(PROTOBUF_TYPE, PROTOBUF_ONEOF_TYPE)
        builder.optional()
        return builder.build()
    }

    private fun toConnectSchemaBuilderForField(
        fieldDescriptor: Descriptors.FieldDescriptor,
        visitedTypes: MutableSet<String>,
    ): SchemaBuilder {
        val protobufType = fieldDescriptor.type

        var schemaBuilder: SchemaBuilder =
            when (protobufType) {
                Descriptors.FieldDescriptor.Type.INT32, SINT32, SFIXED32 -> int32OrConnectMetadataType(fieldDescriptor)

                Descriptors.FieldDescriptor.Type.INT64, SINT64, UINT64, FIXED64, SFIXED64, UINT32, FIXED32 ->
                    SchemaBuilder.int64()

                Descriptors.FieldDescriptor.Type.FLOAT -> SchemaBuilder.float32()
                Descriptors.FieldDescriptor.Type.DOUBLE -> SchemaBuilder.float64()
                Descriptors.FieldDescriptor.Type.BOOL -> SchemaBuilder.bool()

                // ENUM becomes a string in Connect, which has no ENUM; its data is kept in metadata below.
                ENUM, Descriptors.FieldDescriptor.Type.STRING -> SchemaBuilder.string()

                Descriptors.FieldDescriptor.Type.BYTES -> SchemaBuilder.bytes()
                Descriptors.FieldDescriptor.Type.MESSAGE -> messageSchemaBuilder(fieldDescriptor, visitedTypes)
                else -> throw DataException("Invalid Protobuf type passed: $protobufType")
            }

        // Protobuf provides different types of integers; the original type is kept in the Connect
        // schema metadata.
        if (TYPES_TO_ADD_METADATA.contains(protobufType)) {
            schemaBuilder.parameter(PROTOBUF_TYPE, protobufType.name.uppercase())
        }

        if (protobufType == ENUM) {
            // Storing ENUM data as metadata to avoid it being lost in translation.
            schemaBuilder.parameter(PROTOBUF_TYPE, PROTOBUF_ENUM_TYPE)
            for (enumValueDescriptor in fieldDescriptor.enumType.values) {
                schemaBuilder.parameter(
                    PROTOBUF_ENUM_VALUE + enumValueDescriptor.name,
                    enumValueDescriptor.number.toString(),
                )
            }
            schemaBuilder.parameter(PROTOBUF_ENUM_NAME, fieldDescriptor.enumType.fullName)
        }

        if (fieldDescriptor.hasOptionalKeyword()) {
            schemaBuilder.optional()
        }

        if (fieldDescriptor.isRepeated && schemaBuilder.type() != Schema.Type.MAP) {
            schemaBuilder = SchemaBuilder.array(schemaBuilder.build()).optional()
        }

        schemaBuilder.parameter(PROTOBUF_TAG, fieldDescriptor.number.toString())

        return schemaBuilder
    }

    private fun int32OrConnectMetadataType(fieldDescriptor: Descriptors.FieldDescriptor): SchemaBuilder {
        val options = fieldDescriptor.options
        if (options.hasExtension(ProtobufSchemaMetadata.metadataKey) &&
            options.hasExtension(ProtobufSchemaMetadata.metadataValue)
        ) {
            val metadataKey = options.getExtension(ProtobufSchemaMetadata.metadataKey)
            val metadataValue = options.getExtension(ProtobufSchemaMetadata.metadataValue)
            if (metadataKey == CONNECT_SCHEMA_TYPE) {
                if (CONNECT_SCHEMA_INT8 == metadataValue) {
                    return SchemaBuilder.int8()
                } else if (CONNECT_SCHEMA_INT16 == metadataValue) {
                    return SchemaBuilder.int16()
                }
            }
        }
        return SchemaBuilder.int32()
    }

    private fun messageSchemaBuilder(
        fieldDescriptor: Descriptors.FieldDescriptor,
        visitedTypes: MutableSet<String>,
    ): SchemaBuilder {
        if (fieldDescriptor.isMapField) {
            val mapDescriptor = fieldDescriptor.messageType
            return SchemaBuilder.map(
                toConnectSchemaBuilderForField(mapDescriptor.findFieldByName("key"), visitedTypes).optional().build(),
                toConnectSchemaBuilderForField(mapDescriptor.findFieldByName("value"), visitedTypes).optional().build(),
            )
        }

        val fullName = fieldDescriptor.messageType.fullName
        when (fullName) {
            "google.type.Date" -> return Date.builder()
            "google.protobuf.Timestamp" -> return Timestamp.builder()
            "google.type.TimeOfDay" -> return Time.builder()
            "additionalTypes.Decimal" -> return decimalSchemaBuilder(fieldDescriptor)
        }

        if (visitedTypes.contains(fullName)) {
            // Break recursion by creating a placeholder schema for recursive references
            return SchemaBuilder.struct().name(fullName).optional()
        }
        visitedTypes.add(fullName)
        val schemaBuilder = SchemaBuilder.struct().name(fullName)
        for (field in fieldDescriptor.messageType.fields) {
            schemaBuilder.field(field.name, toConnectSchemaForField(field, visitedTypes))
        }
        visitedTypes.remove(fullName)
        return schemaBuilder
    }

    private fun decimalSchemaBuilder(fieldDescriptor: Descriptors.FieldDescriptor): SchemaBuilder {
        val options = fieldDescriptor.options
        if (options.hasExtension(ProtobufSchemaMetadata.metadataKey) &&
            options.hasExtension(ProtobufSchemaMetadata.metadataValue)
        ) {
            val metadataKey = options.getExtension(ProtobufSchemaMetadata.metadataKey)
            val metadataValue = options.getExtension(ProtobufSchemaMetadata.metadataValue)
            if (metadataKey == DECIMAL_SCALE_VALUE) {
                try {
                    val schemaBuilder = Decimal.builder(metadataValue.toInt())
                    schemaBuilder.parameter(DECIMAL_SCALE_VALUE, metadataValue)
                    return schemaBuilder
                } catch (ex: NumberFormatException) {
                    // ignore
                }
            }
        }
        return Decimal.builder(DECIMAL_DEFAULT_SCALE)
    }

    companion object {
        private val TYPES_TO_ADD_METADATA: Set<Descriptors.FieldDescriptor.Type> =
            ImmutableSet
                .builder<Descriptors.FieldDescriptor.Type>()
                .add(SINT32, SFIXED32, UINT32, UINT64, FIXED32, FIXED64, SFIXED64, SINT64)
                .build()
        private const val CONVERTER_VERSION = 1
    }
}
