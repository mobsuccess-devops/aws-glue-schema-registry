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

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getSchemaSimpleName
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.toValidFullName
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.toValidIdentifier
import com.google.protobuf.DescriptorProtos
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException
import java.util.concurrent.atomic.AtomicInteger

/**
 * Builds the fields into the given message and fileDescriptorProto.
 */
object FieldBuilder {
    @JvmStatic
    fun build(
        schema: Schema,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
        messageDescriptorProtoBuilder: DescriptorProtos.DescriptorProto.Builder,
    ) {
        // Sequentially add tag numbers to fields as they appear in the original schema, starting at 1.
        val tagNumber = AtomicInteger(1)
        val syntheticOneofs = ArrayList<String>()
        val fieldBuilderMap = LinkedHashMap<String, DescriptorProtos.FieldDescriptorProto.Builder>()

        for (field in schema.fields()) {
            val fieldSchema = field.schema()
            val fieldName = field.name()

            // Get the corresponding type converter and convert it.
            if (isEnumType(fieldSchema)) {
                val schemaParams = fieldSchema.parameters()
                val enumFullName = schemaParams[PROTOBUF_ENUM_NAME]!!

                val enumDescriptorProtoBuilder =
                    DescriptorProtos.EnumDescriptorProto.newBuilder().setName(getSchemaSimpleName(enumFullName))
                for (parameter in schemaParams.entries) {
                    if (parameter.key.startsWith(PROTOBUF_ENUM_VALUE)) {
                        enumDescriptorProtoBuilder.addValue(
                            DescriptorProtos.EnumValueDescriptorProto
                                .newBuilder()
                                .setName(toValidIdentifier(parameter.key.replace(PROTOBUF_ENUM_VALUE, "")))
                                .setNumber(parameter.value.toInt())
                                .build(),
                        )
                    }
                }

                // Adding the Enum to the protobuf schema file, and defining a field as Enum
                if (isParentLevel(fileDescriptorProtoBuilder.getPackage(), toValidFullName(enumFullName))) {
                    fileDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder)
                } else {
                    messageDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder)
                }
            } else if (Schema.Type.MAP == fieldSchema.type()) {
                val mapEntryName = ProtobufSchemaConverterUtils.toMapEntryName(fieldName)
                messageDescriptorProtoBuilder.addNestedType(
                    buildMap(fieldSchema, mapEntryName, fileDescriptorProtoBuilder, messageDescriptorProtoBuilder),
                )
            } else if (Schema.Type.STRUCT == fieldSchema.type()) {
                val fieldParameters: Map<String, String>? = fieldSchema.parameters()
                if (fieldParameters != null && fieldParameters[PROTOBUF_TYPE] == PROTOBUF_ONEOF_TYPE) {
                    buildOneof(
                        fieldSchema,
                        fieldName,
                        tagNumber,
                        fileDescriptorProtoBuilder,
                        messageDescriptorProtoBuilder,
                        fieldBuilderMap,
                    )
                    continue
                }

                // Convert the Struct type schema to a Protobuf message schema
                val nestedMessageDescriptorProtoBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                val structName: String? = fieldSchema.name()
                nestedMessageDescriptorProtoBuilder.setName(
                    getSchemaSimpleName(structName ?: capitalize(fieldName)),
                )
                build(fieldSchema, fileDescriptorProtoBuilder, nestedMessageDescriptorProtoBuilder)
                // A parent level schema is added as a message type, a nested one as a nested type.
                if (structName != null &&
                    isParentLevel(fileDescriptorProtoBuilder.getPackage(), toValidFullName(structName))
                ) {
                    fileDescriptorProtoBuilder.addMessageType(nestedMessageDescriptorProtoBuilder)
                } else {
                    messageDescriptorProtoBuilder.addNestedType(nestedMessageDescriptorProtoBuilder)
                }
            }

            val fieldDescriptorProtoBuilder =
                getFieldDescriptorProtoBuilder(
                    fieldSchema,
                    fieldName,
                    fileDescriptorProtoBuilder,
                    messageDescriptorProtoBuilder,
                )
            fieldDescriptorProtoBuilder.setNumber(
                tagNumberFromMetadata(fieldSchema.parameters()) ?: tagNumber.getAndIncrement(),
            )
            // Proto3 Optional helps distinguish between non-existing and empty values.
            if (fieldSchema.isOptional && fieldSchema.type() != Schema.Type.ARRAY) {
                syntheticOneofs.add(fieldName)
            }
            fieldBuilderMap[fieldName] = fieldDescriptorProtoBuilder
        }

        // Synthetic oneofs must be ordered after all "real" oneofs
        for (syntheticOneofName in syntheticOneofs) {
            setProto3Optional(fieldBuilderMap[syntheticOneofName]!!, messageDescriptorProtoBuilder)
        }
        for (entry in fieldBuilderMap.entries) {
            messageDescriptorProtoBuilder.addField(entry.value)
        }
    }

    /**
     * A Protobuf map is built from two parts: the map field and the nested type for the map entry.
     * The nested type carries the key as optional field 1, the value as optional field 2, and the
     * MapEntry option set to true.
     */
    private fun buildMap(
        schema: Schema,
        name: String,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
        messageDescriptorProtoBuilder: DescriptorProtos.DescriptorProto.Builder,
    ): DescriptorProtos.DescriptorProto {
        val keyFieldBuilder =
            getFieldDescriptorProtoBuilder(
                schema.keySchema(),
                "key",
                fileDescriptorProtoBuilder,
                messageDescriptorProtoBuilder,
            )
        keyFieldBuilder.setNumber(1)
        val valueFieldBuilder =
            getFieldDescriptorProtoBuilder(
                schema.valueSchema(),
                "value",
                fileDescriptorProtoBuilder,
                messageDescriptorProtoBuilder,
            )
        valueFieldBuilder.setNumber(2)

        val mapBuilder = DescriptorProtos.DescriptorProto.newBuilder().setName(name)
        mapBuilder.addField(keyFieldBuilder.build())
        mapBuilder.addField(valueFieldBuilder.build())

        mapBuilder.mergeOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build())

        return mapBuilder.build()
    }

    /**
     * A Protobuf oneof is built by adding a oneof declaration to the message, then adding each
     * oneof field as an optional field carrying the index of that declaration.
     */
    private fun buildOneof(
        schema: Schema,
        name: String,
        tagNumber: AtomicInteger,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
        messageDescriptorProtoBuilder: DescriptorProtos.DescriptorProto.Builder,
        fieldBuilderMap: MutableMap<String, DescriptorProtos.FieldDescriptorProto.Builder>,
    ) {
        messageDescriptorProtoBuilder.addOneofDecl(
            DescriptorProtos.OneofDescriptorProto.newBuilder().setName(toValidIdentifier(name)).build(),
        )
        for (oneofField in schema.fields()) {
            val oneofFieldDescriptorProtoBuilder =
                getFieldDescriptorProtoBuilder(
                    oneofField.schema(),
                    oneofField.name(),
                    fileDescriptorProtoBuilder,
                    messageDescriptorProtoBuilder,
                )
            oneofFieldDescriptorProtoBuilder.setNumber(
                tagNumberFromMetadata(oneofField.schema().parameters()) ?: tagNumber.getAndIncrement(),
            )
            oneofFieldDescriptorProtoBuilder.setOneofIndex(messageDescriptorProtoBuilder.oneofDeclCount - 1)
            fieldBuilderMap[oneofField.name()] = oneofFieldDescriptorProtoBuilder
        }
    }

    private fun getFieldDescriptorProtoBuilder(
        fieldSchema: Schema,
        fieldName: String,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
        messageDescriptorProtoBuilder: DescriptorProtos.DescriptorProto.Builder,
    ): DescriptorProtos.FieldDescriptorProto.Builder {
        val schemaTypeConverter = ConnectToProtobufTypeConverterFactory.get(fieldSchema)
        val fieldDescriptorProtoBuilder =
            schemaTypeConverter.toProtobufSchema(
                fieldSchema,
                messageDescriptorProtoBuilder,
                fileDescriptorProtoBuilder,
            )

        if (Schema.Type.MAP == fieldSchema.type()) {
            fieldDescriptorProtoBuilder.setTypeName(
                getTypeName(
                    fileDescriptorProtoBuilder.getPackage() + "." + messageDescriptorProtoBuilder.getName() + "." +
                        ProtobufSchemaConverterUtils.toMapEntryName(fieldName),
                ),
            )
        } else if (Schema.Type.STRUCT == fieldSchema.type()) {
            val structName = fieldSchema.name()
            fieldDescriptorProtoBuilder.setTypeName(
                if (structName != null) getTypeName(structName) else toValidIdentifier(capitalize(fieldName)),
            )
        }

        fieldDescriptorProtoBuilder.setName(toValidIdentifier(fieldName))
        return fieldDescriptorProtoBuilder
    }

    /**
     * Kafka Connect converters can pre-assign tag numbers to certain fields through the
     * "awsgsr.protobuf.tag" property; this reads it when present.
     */
    private fun tagNumberFromMetadata(schemaParams: Map<String, String>?): Int? {
        if (schemaParams == null || !schemaParams.containsKey(PROTOBUF_TAG)) {
            return null
        }

        val tag = schemaParams[PROTOBUF_TAG]
        try {
            return tag!!.toInt()
        } catch (e: Exception) {
            throw DataException("Cannot parse invalid Protobuf tag number metadata: $tag")
        }
    }

    /**
     * Proto 3.15+ supports optionals, used here for Connect optional schema fields. A Proto3
     * optional adds a synthetic one-of declaration named after the field, as in `oneof_decl {
     * name: "_foo" }`.
     */
    private fun setProto3Optional(
        fieldBuilder: DescriptorProtos.FieldDescriptorProto.Builder,
        descriptorProtoBuilder: DescriptorProtos.DescriptorProto.Builder,
    ) {
        descriptorProtoBuilder.addOneofDecl(
            DescriptorProtos.OneofDescriptorProto.newBuilder().setName("_" + fieldBuilder.getName()).build(),
        )

        fieldBuilder.setProto3Optional(true)
        fieldBuilder.setOneofIndex(descriptorProtoBuilder.oneofDeclCount - 1)
    }

    /**
     * A schema name combines the package name, the parent level schema simple name when there is
     * one, and the schema simple name itself. For `message A { message B {} } message C {}` the
     * names are "package.A", "package.A.B" and "package.C".
     *
     * @return true when the schema sits at parent level.
     */
    private fun capitalize(fieldName: String): String = fieldName.replaceFirstChar { it.uppercase() }

    private fun isParentLevel(
        packageName: String,
        schemaName: String,
    ): Boolean {
        if (!schemaName.startsWith(packageName)) {
            return false
        }
        val names = schemaName.split(packageName)[1].split(".")
        // A parent level schema such as A or C yields ["", "A"] or ["", "C"], while a nested one
        // such as B yields ["", "A", "B"].
        return names.size <= 2
    }
}
