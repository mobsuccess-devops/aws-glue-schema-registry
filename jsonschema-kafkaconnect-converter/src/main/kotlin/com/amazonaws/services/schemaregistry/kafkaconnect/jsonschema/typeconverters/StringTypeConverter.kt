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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.everit.json.schema.EnumSchema
import org.everit.json.schema.StringSchema

class StringTypeConverter : TypeConverter {
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode {
        // Bound to a typed local so the checkcast is actually emitted: `(value as CharSequence)
        // .toString()` resolves toString() on Any, and the cast can be optimized away — which
        // would silently accept a value the original code rejected with a ClassCastException.
        val charSeq: CharSequence = value as CharSequence
        return TypeConverter.JSON_NODE_FACTORY.textNode(charSeq.toString())
    }

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val parameters = schema.parameters()
        if (parameters != null && parameters.containsKey(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM)) {
            // A HashSet, as in the original: the everit schema compares by value set, and a
            // set that preserves insertion order changes the rendered enum ordering.
            val symbols =
                parameters
                    .filterKeys { it.startsWith(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + ".") }
                    .values
                    .toHashSet<Any>()
            return EnumSchema.builder().possibleValues(symbols)
        }
        return StringSchema.builder()
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any? = value.textValue()

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder {
        val builder = SchemaBuilder.string()

        if (jsonSchema is EnumSchema) {
            builder.parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM, null)
            jsonSchema.possibleValuesAsList.forEach {
                builder.parameter(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ENUM + "." + it, it.toString())
            }
        }

        return builder
    }
}
