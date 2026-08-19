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

class MessageDefinition private constructor(
    private val mMsgType: DescriptorProtos.DescriptorProto,
) {
    // --- public ---

    override fun toString(): String = mMsgType.toString()

    // --- package ---

    internal fun getMessageType(): DescriptorProtos.DescriptorProto = mMsgType

    /**
     * MessageDefinition.Builder
     */
    class Builder internal constructor(
        msgTypeName: String,
    ) {
        private val mMsgTypeBuilder: DescriptorProtos.DescriptorProto.Builder =
            DescriptorProtos.DescriptorProto.newBuilder()
        private var mOneofIndex = 0

        init {
            mMsgTypeBuilder.setName(msgTypeName)
        }

        // --- public ---

        @JvmOverloads
        fun addField(
            label: String?,
            type: String,
            name: String,
            num: Int,
            defaultVal: String?,
            jsonName: String? = null,
            isPacked: Boolean? = null,
        ): Builder {
            val protoLabel = sLabelMap[label]
            doAddField(protoLabel, type, name, num, defaultVal, jsonName, isPacked, null)
            return this
        }

        fun addOneof(oneofName: String): OneofBuilder {
            mMsgTypeBuilder.addOneofDecl(
                DescriptorProtos.OneofDescriptorProto.newBuilder().setName(oneofName).build(),
            )
            return OneofBuilder(this, mOneofIndex++)
        }

        fun addMessageDefinition(msgDef: MessageDefinition): Builder {
            mMsgTypeBuilder.addNestedType(msgDef.getMessageType())
            return this
        }

        fun addEnumDefinition(enumDef: EnumDefinition): Builder {
            mMsgTypeBuilder.addEnumType(enumDef.getEnumType())
            return this
        }

        // Note: added
        fun addReservedName(reservedName: String): Builder {
            mMsgTypeBuilder.addReservedName(reservedName)
            return this
        }

        // Note: added
        fun addReservedRange(
            start: Int,
            end: Int,
        ): Builder {
            val rangeBuilder = DescriptorProtos.DescriptorProto.ReservedRange.newBuilder()
            rangeBuilder.setStart(start).setEnd(end)
            mMsgTypeBuilder.addReservedRange(rangeBuilder.build())
            return this
        }

        // Note: added
        fun setMapEntry(mapEntry: Boolean): Builder {
            val optionsBuilder = DescriptorProtos.MessageOptions.newBuilder()
            optionsBuilder.setMapEntry(mapEntry)
            mMsgTypeBuilder.mergeOptions(optionsBuilder.build())
            return this
        }

        fun build(): MessageDefinition = MessageDefinition(mMsgTypeBuilder.build())

        // --- private ---

        internal fun doAddField(
            label: DescriptorProtos.FieldDescriptorProto.Label?,
            type: String,
            name: String,
            num: Int,
            defaultVal: String?,
            jsonName: String?,
            isPacked: Boolean?,
            oneofBuilder: OneofBuilder?,
        ) {
            val fieldBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder()
            // Note: changed
            if (label != null) {
                fieldBuilder.setLabel(label)
            }
            val primType = sTypeMap[type]
            if (primType != null) {
                fieldBuilder.setType(primType)
            } else {
                fieldBuilder.setTypeName(type)
            }
            fieldBuilder.setName(name).setNumber(num)
            if (defaultVal != null) {
                fieldBuilder.setDefaultValue(defaultVal)
            }
            if (oneofBuilder != null) {
                fieldBuilder.setOneofIndex(oneofBuilder.getIdx())
            }
            if (jsonName != null) {
                fieldBuilder.setJsonName(jsonName)
            }
            if (isPacked != null) {
                val optionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
                optionsBuilder.setPacked(isPacked)
                fieldBuilder.mergeOptions(optionsBuilder.build())
            }
            mMsgTypeBuilder.addField(fieldBuilder.build())
        }
    }

    /**
     * MessageDefinition.OneofBuilder
     */
    class OneofBuilder internal constructor(
        private val mMsgBuilder: Builder,
        private val mIdx: Int,
    ) {
        // --- public ---

        @JvmOverloads
        fun addField(
            type: String,
            name: String,
            num: Int,
            defaultVal: String?,
            jsonName: String? = null,
        ): OneofBuilder {
            mMsgBuilder.doAddField(
                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                type,
                name,
                num,
                defaultVal,
                jsonName,
                null,
                this,
            )
            return this
        }

        fun msgDefBuilder(): Builder = mMsgBuilder

        fun getIdx(): Int = mIdx
    }

    companion object {
        // --- public static ---

        @JvmStatic
        fun newBuilder(msgTypeName: String): Builder = Builder(msgTypeName)

        // --- private static ---

        private val sTypeMap: MutableMap<String, DescriptorProtos.FieldDescriptorProto.Type> = HashMap()
        private val sLabelMap: MutableMap<String, DescriptorProtos.FieldDescriptorProto.Label> = HashMap()

        init {
            sTypeMap["double"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE
            sTypeMap["float"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT
            sTypeMap["int32"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32
            sTypeMap["int64"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64
            sTypeMap["uint32"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32
            sTypeMap["uint64"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64
            sTypeMap["sint32"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32
            sTypeMap["sint64"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64
            sTypeMap["fixed32"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32
            sTypeMap["fixed64"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64
            sTypeMap["sfixed32"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32
            sTypeMap["sfixed64"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64
            sTypeMap["bool"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL
            sTypeMap["string"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING
            sTypeMap["bytes"] = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES
            // sTypeMap.put("enum", FieldDescriptorProto.Type.TYPE_ENUM);
            // sTypeMap.put("message", FieldDescriptorProto.Type.TYPE_MESSAGE);
            // sTypeMap.put("group", FieldDescriptorProto.Type.TYPE_GROUP);

            sLabelMap["optional"] = DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL
            sLabelMap["required"] = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED
            sLabelMap["repeated"] = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED
        }
    }
}
