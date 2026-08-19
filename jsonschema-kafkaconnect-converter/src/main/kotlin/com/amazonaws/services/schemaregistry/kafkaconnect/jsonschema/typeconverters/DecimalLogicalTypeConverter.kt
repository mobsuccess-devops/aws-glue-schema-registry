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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.DecimalFormat
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.StringSchema
import java.math.BigDecimal

class DecimalLogicalTypeConverter : TypeConverter {
    /**
     * DecimalFormat is a Connect logical type that can be represented in numeric or Base64 encoded
     * binary form, as described by KIP-481. Both representations are supported so the converter can
     * integrate with legacy systems and keep internal topic data readable:
     *
     * the decimal field "foo" with value "10.2345" serializes to {"foo": "D3J5"} with BASE64,
     * and to {"foo": 10.2345} with NUMERIC.
     */
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode {
        if (value !is BigDecimal) {
            throw DataException("Invalid type for Decimal, expected BigDecimal but was " + value!!.javaClass)
        }

        return when (config.getDecimalFormat()) {
            DecimalFormat.NUMERIC -> TypeConverter.JSON_NODE_FACTORY.numberNode(value)
            DecimalFormat.BASE64 -> TypeConverter.JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, value))
            else -> throw DataException(
                "Unexpected ${JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG}: ${config.getDecimalFormat()}",
            )
        }
    }

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val connectType = schema.type().getName().lowercase()
        val baseSchemaBuilder =
            if (DecimalFormat.NUMERIC == jsonSchemaDataConfig.getDecimalFormat()) {
                NumberSchema.builder()
            } else {
                StringSchema.builder()
            }
        unprocessedProperties[JsonSchemaConverterConstants.CONNECT_TYPE_PROP] = connectType
        return baseSchemaBuilder
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any {
        if (value.isNumber) {
            return value.decimalValue()
        }
        if (value.isBinary || value.isTextual) {
            try {
                return Decimal.toLogical(schema, value.binaryValue())
            } catch (e: Exception) {
                throw DataException("Invalid bytes for Decimal field", e)
            }
        }

        throw DataException(
            "Invalid type for Decimal, underlying representation should be numeric or bytes but was ${value.nodeType}",
        )
    }

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder = throw DataException("Invalid type for Decimal, underlying type should be numeric or bytes.")
}
