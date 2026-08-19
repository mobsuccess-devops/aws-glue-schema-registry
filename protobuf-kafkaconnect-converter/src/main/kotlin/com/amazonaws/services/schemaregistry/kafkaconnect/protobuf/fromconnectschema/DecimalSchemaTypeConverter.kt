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

import additionalTypes.Decimals
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_SCALE_VALUE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.METADATA_IMPORT
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE
import metadata.ProtobufSchemaMetadata
import org.apache.kafka.connect.data.Schema

class DecimalSchemaTypeConverter : SchemaTypeConverter {
    override fun toProtobufSchema(
        schema: Schema,
        descriptorProto: DescriptorProtos.DescriptorProto.Builder,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
    ): DescriptorProtos.FieldDescriptorProto.Builder {
        val typeName = getTypeName(Decimals.getDescriptor().messageTypes[0].fullName)
        val builder =
            DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(typeName)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)

        addImportToProtobufSchema(fileDescriptorProtoBuilder, DECIMAL_IMPORT)

        if (schema.parameters().containsKey(DECIMAL_SCALE_VALUE)) {
            addImportToProtobufSchema(fileDescriptorProtoBuilder, METADATA_IMPORT)

            val keyOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
            keyOptionsBuilder.setExtension(ProtobufSchemaMetadata.metadataKey, DECIMAL_SCALE_VALUE)
            builder.mergeOptions(keyOptionsBuilder.build())

            val valueOptionsBuilder = DescriptorProtos.FieldOptions.newBuilder()
            valueOptionsBuilder.setExtension(
                ProtobufSchemaMetadata.metadataValue,
                schema.parameters()[DECIMAL_SCALE_VALUE],
            )
            builder.mergeOptions(valueOptionsBuilder.build())
        }

        return builder
    }

    companion object {
        private const val DECIMAL_IMPORT = "additionalTypes/decimal.proto"
    }
}
