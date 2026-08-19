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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp

class TimeSchemaTypeConverter : SchemaTypeConverter {
    override fun toProtobufSchema(
        schema: Schema,
        descriptorProto: DescriptorProtos.DescriptorProto.Builder,
        fileDescriptorProtoBuilder: DescriptorProtos.FileDescriptorProto.Builder,
    ): DescriptorProtos.FieldDescriptorProto.Builder {
        var typename = "."
        if (Date.SCHEMA.name() == schema.name()) {
            typename += com.google.type.Date.getDescriptor().fullName
            addImportToProtobufSchema(fileDescriptorProtoBuilder, DATE_PROTO_IMPORT)
        } else if (Timestamp.SCHEMA.name() == schema.name()) {
            typename += com.google.protobuf.Timestamp.getDescriptor().fullName
            addImportToProtobufSchema(fileDescriptorProtoBuilder, TIMESTAMP_PROTO_IMPORT)
        } else if (Time.SCHEMA.name() == schema.name()) {
            typename += com.google.type.TimeOfDay.getDescriptor().fullName
            addImportToProtobufSchema(fileDescriptorProtoBuilder, TIMEOFDAY_PROTO_IMPORT)
        }

        return DescriptorProtos.FieldDescriptorProto
            .newBuilder()
            .setType(TYPE_MESSAGE)
            .setTypeName(typename)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
    }

    companion object {
        private const val DATE_PROTO_IMPORT = "google/type/date.proto"
        private const val TIMESTAMP_PROTO_IMPORT = "google/protobuf/timestamp.proto"
        private const val TIMEOFDAY_PROTO_IMPORT = "google/type/timeofday.proto"
    }
}
