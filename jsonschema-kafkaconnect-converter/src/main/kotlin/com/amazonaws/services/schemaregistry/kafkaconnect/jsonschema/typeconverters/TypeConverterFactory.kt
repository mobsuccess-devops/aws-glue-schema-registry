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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.everit.json.schema.ArraySchema
import org.everit.json.schema.BooleanSchema
import org.everit.json.schema.EnumSchema
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.ObjectSchema
import org.everit.json.schema.StringSchema
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory to create a new instance of TypeConverter.
 */
class TypeConverterFactory {
    private val typeConverterMap: MutableMap<String, TypeConverter> = ConcurrentHashMap()

    /**
     * Returns a specific TypeConverter based on the connect type property or the json schema type.
     */
    fun get(
        jsonSchema: org.everit.json.schema.Schema,
        connectType: String?,
    ): TypeConverter? = when (jsonSchema) {
        is BooleanSchema -> get(Schema.Type.BOOLEAN)
        // If no connect type passed then assume that connect schema is for FLOAT64 type data
        is NumberSchema ->
            if (connectType == null) {
                get(Schema.Type.valueOf("FLOAT64"))
            } else {
                get(Schema.Type.valueOf(connectType.uppercase()))
            }
        // Order matters and mirrors the original instanceof chain: EnumSchema extends
        // StringSchema, so the StringSchema test has to come first.
        is StringSchema -> if ("bytes" == connectType) get(Schema.Type.BYTES) else get(Schema.Type.STRING)
        is EnumSchema -> get(Schema.Type.STRING)
        is ArraySchema -> if ("map" == connectType) get(Schema.Type.MAP) else get(Schema.Type.ARRAY)
        is ObjectSchema -> if ("map" == connectType) get(Schema.Type.MAP) else get(Schema.Type.STRUCT)
        else -> null
    }

    /**
     * Returns a specific TypeConverter based on the logical type from the schema name, or its type.
     */
    fun get(schema: Schema): TypeConverter {
        val name = schema.name()
        if (name != null) {
            get(name)?.let { return it }
        }
        return get(schema.type())
    }

    /**
     * Lazy initializes and returns a specific TypeConverter instance.
     */
    fun get(connectType: Schema.Type): TypeConverter {
        val factory: () -> TypeConverter =
            when (connectType) {
                Schema.Type.INT8 -> ::Int8TypeConverter
                Schema.Type.INT16 -> ::Int16TypeConverter
                Schema.Type.INT32 -> ::Int32TypeConverter
                Schema.Type.INT64 -> ::Int64TypeConverter
                Schema.Type.FLOAT32 -> ::Float32TypeConverter
                Schema.Type.FLOAT64 -> ::Float64TypeConverter
                Schema.Type.BOOLEAN -> ::BooleanTypeConverter
                Schema.Type.BYTES -> ::BytesTypeConverter
                Schema.Type.STRING -> ::StringTypeConverter
                Schema.Type.ARRAY -> ::ArrayTypeConverter
                Schema.Type.MAP -> ::MapTypeConverter
                Schema.Type.STRUCT -> ::StructTypeConverter
                else -> throw DataException("Unsupported connect type: ${connectType.getName()}")
            }
        return typeConverterMap.computeIfAbsent(connectType.getName()) { factory() }
    }

    /**
     * Lazy initializes and returns the TypeConverter of a Kafka Connect logical type, or null when
     * the name does not designate one.
     */
    fun get(logicalName: String): TypeConverter? {
        val factory: () -> TypeConverter =
            when (logicalName) {
                Decimal.LOGICAL_NAME -> ::DecimalLogicalTypeConverter
                Date.LOGICAL_NAME -> ::DateLogicalTypeConverter
                Time.LOGICAL_NAME -> ::TimeLogicalTypeConverter
                Timestamp.LOGICAL_NAME -> ::TimestampLogicalTypeConverter
                else -> return null
            }
        return typeConverterMap.computeIfAbsent(logicalName) { factory() }
    }
}
