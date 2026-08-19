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
import com.google.protobuf.util.Timestamps
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import java.util.Calendar
import java.util.TimeZone

class TimeDataConverter : DataConverter {
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
        if (Date.SCHEMA.name() == schema.name()) {
            val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            cal.time = value as java.util.Date
            return com.google.type.Date
                .newBuilder()
                .setDay(cal.get(Calendar.DAY_OF_MONTH))
                // Months start at 0
                .setMonth(cal.get(Calendar.MONTH) + 1)
                .setYear(cal.get(Calendar.YEAR))
                .build()
        } else if (Time.SCHEMA.name() == schema.name()) {
            val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            cal.time = value as java.util.Date
            return com.google.type.TimeOfDay
                .newBuilder()
                .setHours(cal.get(Calendar.HOUR_OF_DAY))
                .setMinutes(cal.get(Calendar.MINUTE))
                .setSeconds(cal.get(Calendar.SECOND))
                // Converting milliseconds to nanoseconds
                .setNanos(cal.get(Calendar.MILLISECOND) * 1000000)
                .build()
        } else if (Timestamp.SCHEMA.name() == schema.name()) {
            return Timestamps.fromMillis(Timestamp.fromLogical(schema, value as java.util.Date))
        }

        throw DataException("Invalid schema type ${schema.type()} for value ${value!!.javaClass}")
    }
}
