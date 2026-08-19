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
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getSchemaSimpleName
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName
import com.google.protobuf.DescriptorProtos
import org.apache.kafka.connect.data.Schema

class EnumSchemaTypeConverter : SchemaTypeConverter {
    override fun toProtobufSchema(
        schema: Schema,
        descriptorProto: DescriptorProtos.DescriptorProto.Builder,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
    ): DescriptorProtos.FieldDescriptorProto.Builder {
        // Defining the Enum in protobuf schema form
        val enumFullName = schema.parameters()[PROTOBUF_ENUM_NAME]!!

        return DescriptorProtos.FieldDescriptorProto
            .newBuilder()
            .setName(getSchemaSimpleName(enumFullName))
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
            .setTypeName(getTypeName(enumFullName))
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
    }
}
