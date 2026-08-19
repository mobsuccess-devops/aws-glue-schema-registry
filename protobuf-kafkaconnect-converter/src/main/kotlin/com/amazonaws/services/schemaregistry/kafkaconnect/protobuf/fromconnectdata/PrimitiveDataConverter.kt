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

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.Descriptors.FieldDescriptor.Type.FIXED32
import com.google.protobuf.Descriptors.FieldDescriptor.Type.UINT32
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException
import java.nio.ByteBuffer

class PrimitiveDataConverter : DataConverter {
    override fun toProtobufData(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any?,
        fieldDescriptor: Descriptors.FieldDescriptor?,
        messageBuilder: Message.Builder,
    ) {
        messageBuilder.setField(fieldDescriptor, toProtobufData(fileDescriptor, schema, value, fieldDescriptor))
    }

    override fun toProtobufData(
        fileDescriptor: Descriptors.FileDescriptor,
        schema: Schema,
        value: Any?,
        fieldDescriptor: Descriptors.FieldDescriptor?,
    ): Any {
        try {
            // Each cast is bound to a typed local so the checkcast is actually emitted: assigning
            // straight into the `Any` result of the when lets Kotlin optimize it away, and a
            // wrong-typed value would then slip through instead of raising ClassCastException.
            return when (schema.type()) {
                Schema.Type.INT8 -> {
                    val byteValue: Byte = value as Byte
                    byteValue.toInt()
                }
                Schema.Type.INT16 -> {
                    val shortValue: Short = value as Short
                    shortValue.toInt()
                }
                Schema.Type.INT32 -> {
                    val intValue: Int = value as Int
                    intValue
                }
                Schema.Type.INT64 ->
                    if (INT32_METADATA_TYPES.contains(fieldDescriptor!!.type)) {
                        // If type metadata is set to one of the 32-bit types.
                        val numberValue: Number = value as Number
                        numberValue.toLong().toInt()
                    } else {
                        val longValue: Long = value as Long
                        longValue
                    }
                Schema.Type.FLOAT32 -> {
                    val floatValue: Float = value as Float
                    floatValue
                }
                Schema.Type.FLOAT64 -> {
                    val doubleValue: Double = value as Double
                    doubleValue
                }
                Schema.Type.BOOLEAN -> {
                    val boolValue: Boolean = value as Boolean
                    boolValue
                }
                Schema.Type.STRING -> {
                    val stringValue: String = value as String
                    stringValue
                }
                Schema.Type.BYTES -> {
                    val bytesValue = if (value is ByteArray) ByteBuffer.wrap(value) else value as ByteBuffer
                    ByteString.copyFrom(bytesValue)
                }
                else -> throw DataException(
                    "Unknown schema type: ${schema.type()} for field ${fieldDescriptor?.name}",
                )
            }
        } catch (e: ClassCastException) {
            throw DataException("Invalid schema type ${schema.type()} for value ${value!!.javaClass}")
        }
    }

    companion object {
        private val INT32_METADATA_TYPES = listOf(UINT32, FIXED32)
    }
}
