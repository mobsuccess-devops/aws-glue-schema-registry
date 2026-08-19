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

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isTimeType
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema

/**
 * Provides a converter instance that can convert the specific connect type to a Protobuf type.
 */
object ConnectToProtobufTypeConverterFactory {
    @JvmStatic
    fun get(connectSchema: Schema): SchemaTypeConverter {
        val connectType = connectSchema.type()

        return when {
            isEnumType(connectSchema) -> EnumSchemaTypeConverter()
            isTimeType(connectSchema) -> TimeSchemaTypeConverter()
            Decimal.schema(DECIMAL_DEFAULT_SCALE).name() == connectSchema.name() -> DecimalSchemaTypeConverter()
            connectType.isPrimitive -> PrimitiveSchemaTypeConverter()
            connectType == Schema.Type.ARRAY -> ArraySchemaTypeConverter()
            connectType == Schema.Type.MAP -> MapSchemaTypeConverter()
            connectType == Schema.Type.STRUCT -> StructSchemaTypeConverter()
            else -> throw IllegalArgumentException("Unrecognized connect type: $connectType")
        }
    }
}
