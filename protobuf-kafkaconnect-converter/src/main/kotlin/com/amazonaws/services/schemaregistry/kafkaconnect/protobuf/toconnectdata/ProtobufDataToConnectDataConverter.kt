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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata

import additionalTypes.Decimals
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.util.Timestamps
import com.google.type.TimeOfDay
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode

/**
 * Converts Protobuf data to Connect data corresponding to the translated schema.
 */
class ProtobufDataToConnectDataConverter {
    fun toConnectData(
        message: Message,
        connectSchema: Schema,
    ): Any {
        val data = Struct(connectSchema)

        for (field in connectSchema.fields()) {
            if (field.schema().type() == Schema.Type.STRUCT &&
                field.schema().parameters().containsKey(PROTOBUF_TYPE) &&
                field.schema().parameters()[PROTOBUF_TYPE] == PROTOBUF_ONEOF_TYPE
            ) {
                val oneof = Struct(field.schema())
                for (oneofField in field.schema().fields()) {
                    toConnectDataField(oneofField, message, oneof)
                }
                data.put(field, oneof)
            } else {
                toConnectDataField(field, message, data)
            }
        }

        return data
    }

    private fun toConnectDataField(
        connectField: Field,
        message: Message,
        data: Struct,
    ) {
        val connectSchema = connectField.schema()
        val fieldDescriptor =
            getFieldByName(message, connectField.name())
                ?: throw DataException("Protobuf schema doesn't contain the connect field: ${connectField.name()}")

        if (fieldDescriptor.realContainingOneof != null && !message.hasField(fieldDescriptor)) {
            // Skip the NONE or NOT_SET oneof field
            return
        }
        val value = message.getField(fieldDescriptor)
        // Unfortunately Protobuf 3 has a complex way to check for optionals.
        val isOptionalFieldNotSet = fieldDescriptor.hasOptionalKeyword() && !message.hasField(fieldDescriptor)

        if (value == null || isOptionalFieldNotSet) {
            data.put(connectField, null)
            return
        }

        try {
            data.put(connectField, toConnectDataField(connectSchema, value))
        } catch (e: Exception) {
            throw DataException(
                "Error converting value: \"$value\" (Java Type: ${value.javaClass}, " +
                    "Protobuf type: ${fieldDescriptor.type}) to Connect type: ${connectSchema.type()}",
                e,
            )
        }
    }

    private fun toConnectDataField(
        schema: Schema,
        value: Any,
    ): Any? {
        try {
            if (Date.SCHEMA.name() == schema.name()) {
                val date = com.google.type.Date.parseFrom((value as Message).toByteArray())
                return ProtobufSchemaConverterUtils.convertFromGoogleDate(date)
            }
            if (Timestamp.SCHEMA.name() == schema.name()) {
                val timestamp = com.google.protobuf.Timestamp.parseFrom((value as Message).toByteArray())
                return Timestamp.toLogical(schema, Timestamps.toMillis(timestamp))
            }
            if (Time.SCHEMA.name() == schema.name()) {
                val time = TimeOfDay.parseFrom((value as Message).toByteArray())
                return ProtobufSchemaConverterUtils.convertFromGoogleTime(time)
            }
            if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name() == schema.name()) {
                return fromDecimalProto(Decimals.Decimal.parseFrom((value as Message).toByteArray()))
            }
        } catch (e: InvalidProtocolBufferException) {
            throw DataException("Failed to parse protobuf message for schema: ${schema.name()}", e)
        }

        // Each cast is bound to a typed local so the checkcast is actually emitted rather than
        // optimized away against the Any result.
        return when (schema.type()) {
            Schema.Type.INT8 -> (value as Number).toByte()
            Schema.Type.INT16 -> (value as Number).toShort()
            Schema.Type.INT32 -> (value as Number).toInt()
            Schema.Type.INT64 -> {
                val number = value as Number
                if (value is Long) number.toLong() else Integer.toUnsignedLong(number.toInt())
            }
            Schema.Type.FLOAT32 -> (value as Number).toFloat()
            Schema.Type.FLOAT64 -> (value as Number).toDouble()
            Schema.Type.BOOLEAN -> {
                val boolValue: Boolean = value as Boolean
                boolValue
            }
            Schema.Type.STRING ->
                if (value is Enum<*> || value is Descriptors.EnumValueDescriptor) {
                    value.toString()
                } else {
                    val strValue: String = value as String
                    strValue
                }
            Schema.Type.BYTES -> {
                val byteString: ByteString = value as ByteString
                byteString.toByteArray()
            }
            Schema.Type.ARRAY -> {
                val valueSchema = schema.valueSchema()
                (value as Collection<*>).map { toConnectDataField(valueSchema, it!!) }
            }
            Schema.Type.MAP -> {
                @Suppress("UNCHECKED_CAST")
                val original = value as Collection<Message>
                original.associate {
                    toConnectDataField(schema.keySchema(), getMapField(it, "key")) to
                        toConnectDataField(schema.valueSchema(), getMapField(it, "value"))
                }
            }
            Schema.Type.STRUCT -> toConnectData(value as Message, schema.schema())
            else -> throw DataException("Cannot convert unrecognized schema type: ${schema.type()}")
        }
    }

    private fun getMapField(
        mapEntry: Message,
        fieldName: String,
    ): Any = mapEntry.getField(getFieldByName(mapEntry, fieldName))

    private fun getFieldByName(
        message: Message,
        fieldName: String,
    ): Descriptors.FieldDescriptor? = message.descriptorForType.findFieldByName(fieldName)

    companion object {
        @JvmStatic
        fun fromDecimalProto(decimal: Decimals.Decimal): BigDecimal {
            val precisionMathContext = MathContext(decimal.precision, RoundingMode.UNNECESSARY)
            val units = BigDecimal(decimal.units, precisionMathContext)

            var fractionalPart = BigDecimal(decimal.fraction, precisionMathContext)
            val fractionalUnits = BigDecimal(1000000000, precisionMathContext)
            // Set the right scale for the fractional part, ignoring digits beyond the scale.
            fractionalPart =
                fractionalPart
                    .divide(fractionalUnits, precisionMathContext)
                    .setScale(decimal.scale, RoundingMode.UNNECESSARY)

            return units.add(fractionalPart)
        }
    }
}
