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

import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema

interface DataConverter {
    /** Sets the field value using the value returned by the toProtobufData overload below. */
    fun toProtobufData(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any?,
        fieldDescriptor: Descriptors.FieldDescriptor?,
        messageBuilder: Message.Builder,
    )

    /** Returns the value produced by the data conversion. */
    fun toProtobufData(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any?,
        fieldDescriptor: Descriptors.FieldDescriptor?,
    ): Any
}
