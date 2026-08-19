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

import additionalTypes.Decimals
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema
import java.math.BigDecimal

class DecimalDataConverter : DataConverter {
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
    ): Any = fromBigDecimal(value as BigDecimal)

    companion object {
        @JvmStatic
        fun fromBigDecimal(bigDecimal: BigDecimal): Decimals.Decimal = Decimals.Decimal
            .newBuilder()
            .setUnits(bigDecimal.toLong())
            .setFraction(
                bigDecimal.remainder(BigDecimal.ONE).multiply(BigDecimal.valueOf(1000000000)).toInt(),
            ).setPrecision(bigDecimal.precision())
            .setScale(bigDecimal.scale())
            .build()
    }
}
