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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException

/**
 * Converts Connect data to Protobuf data according to the Protobuf schema.
 */
class ConnectDataToProtobufDataConverter {
    fun convert(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any,
    ): Message {
        // TODO: add caching of fileDescriptor to messages by name map
        val allMessagesByName = DescriptorTree.parseAllDescriptors(fileDescriptor)
        val pathName = getPathName(fileDescriptor.getPackage(), schema.name())

        return convert(fileDescriptor, schema, value, allMessagesByName[pathName])
    }

    private fun convert(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any,
        descriptor: Descriptors.Descriptor?,
    ): Message {
        val data = value as Struct
        val dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor)

        for (field in schema.fields()) {
            val fieldValue = data.get(field)

            if (field.schema().type() == Schema.Type.MAP) {
                addMapField(fileDescriptor, dynamicMessageBuilder, field, fieldValue)
            } else if (field.schema().type() == Schema.Type.STRUCT) {
                if (field.schema().parameters()?.get(PROTOBUF_TYPE) == PROTOBUF_ONEOF_TYPE) {
                    for (oneofField in field.schema().fields()) {
                        addField(fileDescriptor, dynamicMessageBuilder, oneofField, (fieldValue as Struct).get(oneofField))
                    }
                    continue
                }
                val fieldDescriptor = dynamicMessageBuilder.descriptorForType.findFieldByName(field.name())
                dynamicMessageBuilder.setField(
                    fieldDescriptor,
                    convert(fileDescriptor, field.schema(), fieldValue, fieldDescriptor.messageType),
                )
            } else {
                addField(fileDescriptor, dynamicMessageBuilder, field, fieldValue)
            }
        }

        return dynamicMessageBuilder.build()
    }

    private fun getPathName(
        packageName: String,
        schemaName: String,
    ): String = if (schemaName.startsWith(packageName)) schemaName.replace(packageName, "") else ".$schemaName"

    private fun addField(
        fileDescriptor: Descriptors.FileDescriptor,
        builder: Message.Builder,
        field: Field,
        value: Any?,
    ) {
        val protobufFieldName = field.name()
        val fieldDescriptor = builder.descriptorForType.findFieldByName(protobufFieldName)
        val schema = field.schema()

        if (value == null) {
            if (!schema.isOptional) {
                throw DataException(
                    "Field data cannot be null for non-optional field. ${schema.type()}: $protobufFieldName",
                )
            }
            return
        }

        ConnectDataToProtobufDataConverterFactory
            .get(schema)
            .toProtobufData(fileDescriptor, schema, value, fieldDescriptor, builder)
    }

    private fun addMapField(
        fileDescriptor: Descriptors.FileDescriptor,
        builder: Message.Builder,
        field: Field,
        value: Any?,
    ) {
        val protobufFieldName = field.name()
        val schema = field.schema()
        val mapDescriptor =
            builder.descriptorForType.findNestedTypeByName(
                ProtobufSchemaConverterUtils.toMapEntryName(protobufFieldName),
            )

        val mapBuilder = DynamicMessage.newBuilder(mapDescriptor)
        val keyFieldDescriptor = mapDescriptor.findFieldByName("key")
        val valueFieldDescriptor = mapDescriptor.findFieldByName("value")
        val keyDataConverter = ConnectDataToProtobufDataConverterFactory.get(schema.keySchema())
        val valueDataConverter = ConnectDataToProtobufDataConverterFactory.get(schema.valueSchema())

        for (entry in (value as Map<*, *>).entries) {
            keyDataConverter.toProtobufData(fileDescriptor, schema.keySchema(), entry.key, keyFieldDescriptor, mapBuilder)
            valueDataConverter.toProtobufData(
                fileDescriptor,
                schema.valueSchema(),
                entry.value,
                valueFieldDescriptor,
                mapBuilder,
            )

            builder.addRepeatedField(
                builder.descriptorForType.findFieldByName(field.name()),
                mapBuilder.build(),
            )
        }
    }
}
