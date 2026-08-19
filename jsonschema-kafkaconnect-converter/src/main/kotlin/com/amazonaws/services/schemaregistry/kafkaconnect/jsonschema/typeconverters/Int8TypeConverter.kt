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
import org.everit.json.schema.NumberSchema

class Int8TypeConverter : TypeConverter {
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode = TypeConverter.JSON_NODE_FACTORY.numberNode(value as Byte)

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val connectType = schema.type().getName().lowercase()
        val baseSchemaBuilder = NumberSchema.builder().requiresInteger(true)
        unprocessedProperties[JsonSchemaConverterConstants.CONNECT_TYPE_PROP] = connectType
        return baseSchemaBuilder
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any = (value.intValue()).toByte()

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder = SchemaBuilder.int8()
}
