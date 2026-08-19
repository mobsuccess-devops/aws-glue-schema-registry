/*
 * Copyright 2021 Red Hat
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import metadata.ProtobufSchemaMetadata

/**
 * @author Fabian Martinez
 */
class ProtobufMessage {
    private val descriptorProtoBuilder: DescriptorProto.Builder = DescriptorProto.newBuilder()

    fun protoBuilder(): DescriptorProto.Builder = descriptorProtoBuilder

    fun build(): DescriptorProto = descriptorProtoBuilder.build()

    fun addField(
        label: String?,
        type: String?,
        typeName: String?,
        name: String,
        num: Int,
        defaultVal: String?,
        jsonName: String?,
        isDeprecated: Boolean?,
        isPacked: Boolean?,
        ctype: DescriptorProtos.FieldOptions.CType?,
        jsType: DescriptorProtos.FieldOptions.JSType?,
        metadataKey: String?,
        metadataValue: String?,
        oneOfIndex: Int?,
        isProto3Optional: Boolean?,
    ) {
        descriptorProtoBuilder.addField(
            buildFieldDescriptorProto(
                label, type, typeName, name, num, defaultVal, jsonName, isDeprecated,
                isPacked, ctype, jsType, metadataKey, metadataValue, oneOfIndex, isProto3Optional,
            ),
        )
    }

    companion object {
        private val fieldDescriptorTypes: MutableMap<String, FieldDescriptorProto.Type> = HashMap()
        private val fieldDescriptorLabels: MutableMap<String, FieldDescriptorProto.Label> = HashMap()

        init {
            fieldDescriptorLabels["optional"] = FieldDescriptorProto.Label.LABEL_OPTIONAL
            fieldDescriptorLabels["required"] = FieldDescriptorProto.Label.LABEL_REQUIRED
            fieldDescriptorLabels["repeated"] = FieldDescriptorProto.Label.LABEL_REPEATED

            fieldDescriptorTypes["double"] = FieldDescriptorProto.Type.TYPE_DOUBLE
            fieldDescriptorTypes["float"] = FieldDescriptorProto.Type.TYPE_FLOAT
            fieldDescriptorTypes["int32"] = FieldDescriptorProto.Type.TYPE_INT32
            fieldDescriptorTypes["int64"] = FieldDescriptorProto.Type.TYPE_INT64
            fieldDescriptorTypes["uint32"] = FieldDescriptorProto.Type.TYPE_UINT32
            fieldDescriptorTypes["uint64"] = FieldDescriptorProto.Type.TYPE_UINT64
            fieldDescriptorTypes["sint32"] = FieldDescriptorProto.Type.TYPE_SINT32
            fieldDescriptorTypes["sint64"] = FieldDescriptorProto.Type.TYPE_SINT64
            fieldDescriptorTypes["fixed32"] = FieldDescriptorProto.Type.TYPE_FIXED32
            fieldDescriptorTypes["fixed64"] = FieldDescriptorProto.Type.TYPE_FIXED64
            fieldDescriptorTypes["sfixed32"] = FieldDescriptorProto.Type.TYPE_SFIXED32
            fieldDescriptorTypes["sfixed64"] = FieldDescriptorProto.Type.TYPE_SFIXED64
            fieldDescriptorTypes["bool"] = FieldDescriptorProto.Type.TYPE_BOOL
            fieldDescriptorTypes["string"] = FieldDescriptorProto.Type.TYPE_STRING
            fieldDescriptorTypes["bytes"] = FieldDescriptorProto.Type.TYPE_BYTES
            fieldDescriptorTypes["enum"] = FieldDescriptorProto.Type.TYPE_ENUM
            fieldDescriptorTypes["message"] = FieldDescriptorProto.Type.TYPE_MESSAGE
            fieldDescriptorTypes["group"] = FieldDescriptorProto.Type.TYPE_GROUP
        }

        @JvmStatic
        fun buildFieldDescriptorProto(
            label: String?,
            type: String?,
            typeName: String?,
            name: String,
            num: Int,
            defaultVal: String?,
            jsonName: String?,
            isDeprecated: Boolean?,
            isPacked: Boolean?,
            ctype: DescriptorProtos.FieldOptions.CType?,
            jsType: DescriptorProtos.FieldOptions.JSType?,
            metadataKey: String?,
            metadataValue: String?,
            oneOfIndex: Int?,
            isProto3Optional: Boolean?,
        ): FieldDescriptorProto {
            val fieldBuilder = FieldDescriptorProto.newBuilder()
            val protoLabel = fieldDescriptorLabels[label]
            if (label != null) {
                fieldBuilder.setLabel(protoLabel)
            }
            val primType = fieldDescriptorTypes[typeName]
            if (primType != null) {
                fieldBuilder.setType(primType)
            } else {
                var fieldDescriptorType: FieldDescriptorProto.Type? = null
                if (type != null) {
                    fieldDescriptorType = fieldDescriptorTypes[type]
                    fieldBuilder.setType(fieldDescriptorType)
                }
                if (fieldDescriptorType != null &&
                    (
                        fieldDescriptorType == FieldDescriptorProto.Type.TYPE_MESSAGE ||
                            fieldDescriptorType == FieldDescriptorProto.Type.TYPE_ENUM
                        )
                ) {
                    // References to other nested messages / enums / google.protobuf types start with "."
                    // See https://developers.google.com/protocol-buffers/docs/proto#packages_and_name_resolution
                    fieldBuilder.setTypeName(if (typeName!!.startsWith(".")) typeName else ".$typeName")
                } else {
                    fieldBuilder.setTypeName(typeName)
                }
            }
            fieldBuilder.setName(name).setNumber(num)
            if (defaultVal != null) {
                fieldBuilder.setDefaultValue(defaultVal)
            }
            if (oneOfIndex != null) {
                fieldBuilder.setOneofIndex(oneOfIndex)
            }
            if (jsonName != null) {
                fieldBuilder.setJsonName(jsonName)
            }

            if (isDeprecated != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setDeprecated(isDeprecated)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (isPacked != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setPacked(isPacked)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (ctype != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setCtype(ctype)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (metadataKey != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, metadataKey)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (metadataValue != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setExtension(ProtobufSchemaMetadata.metadataValue, metadataValue)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (jsType != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setJstype(jsType)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }

            if (isProto3Optional != null) {
                fieldBuilder.setProto3Optional(isProto3Optional)
            }
            return fieldBuilder.build()
        }
    }
}
