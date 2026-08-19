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

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.google.common.base.CaseFormat.LOWER_UNDERSCORE
import com.google.common.base.CaseFormat.UPPER_CAMEL
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import java.util.Calendar
import java.util.TimeZone

object ProtobufSchemaConverterUtils {
    private const val MAP_ENTRY_SUFFIX = "Entry"

    @JvmStatic
    fun getTypeName(typeName: String): String = if (typeName.startsWith(".")) typeName else ".$typeName"

    @JvmStatic
    fun toMapEntryName(name: String): String {
        var s = name
        if (s.contains("_")) {
            s = LOWER_UNDERSCORE.to(UPPER_CAMEL, s)
        }
        s += MAP_ENTRY_SUFFIX
        return s.substring(0, 1).uppercase() + s.substring(1)
    }

    @JvmStatic
    fun getSchemaSimpleName(schemaName: String): String = schemaName.split(".").last()

    @JvmStatic
    fun isEnumType(schema: Schema): Boolean = schema.type() == Schema.Type.STRING &&
        schema.parameters() != null &&
        schema.parameters().containsKey(PROTOBUF_TYPE) &&
        PROTOBUF_ENUM_TYPE == schema.parameters()[PROTOBUF_TYPE]

    @JvmStatic
    fun isTimeType(schema: Schema): Boolean = Date.SCHEMA.name() == schema.name() ||
        Timestamp.SCHEMA.name() == schema.name() ||
        Time.SCHEMA.name() == schema.name()

    @JvmStatic
    fun convertFromGoogleDate(date: com.google.type.Date): java.util.Date {
        val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        cal.isLenient = false
        cal.set(Calendar.YEAR, date.year)
        // Months start at 0, not 1
        cal.set(Calendar.MONTH, date.month - 1)
        cal.set(Calendar.DAY_OF_MONTH, date.day)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        return cal.time
    }

    @JvmStatic
    fun convertFromGoogleTime(time: com.google.type.TimeOfDay): java.util.Date {
        val cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        // Year, month and day are hardcoded to match the Unix epoch.
        cal.set(Calendar.YEAR, 1969)
        cal.set(Calendar.MONTH, 11)
        cal.set(Calendar.DAY_OF_MONTH, 32)
        cal.set(Calendar.HOUR_OF_DAY, time.hours)
        cal.set(Calendar.MINUTE, time.minutes)
        cal.set(Calendar.SECOND, time.seconds)
        cal.set(Calendar.MILLISECOND, time.nanos / 1000000)
        return cal.time
    }
}
