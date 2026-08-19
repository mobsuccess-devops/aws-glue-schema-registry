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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectSchemaToJsonSchemaConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.ConnectValueToJsonNodeConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonNodeToConnectValueConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaToConnectSchemaConverter
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException
import org.everit.json.schema.ArraySchema

class ArrayTypeConverter : TypeConverter {
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode {
        val connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(config)
        val list = TypeConverter.JSON_NODE_FACTORY.arrayNode()
        val valueSchema = schema?.valueSchema()
        for (elem in value as Collection<*>) {
            list.add(connectValueToJsonNodeConverter.convertToJson(valueSchema, elem))
        }
        return list
    }

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val connectSchemaToJsonSchemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)
        return ArraySchema
            .builder()
            .allItemSchema(connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false))
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any {
        val jsonNodeToConnectValueConverter = JsonNodeToConnectValueConverter(jsonSchemaDataConfig)
        val elemSchema = schema?.valueSchema()
        val result = ArrayList<Any?>()
        for (elem in value) {
            result.add(jsonNodeToConnectValueConverter.toConnectValue(elemSchema, elem))
        }
        return result
    }

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder {
        val jsonSchemaToConnectSchemaConverter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig)
        val itemsSchema =
            (jsonSchema as ArraySchema).allItemSchema
                ?: throw DataException("Array schema did not specify the items type")
        return SchemaBuilder.array(jsonSchemaToConnectSchemaConverter.toConnectSchema(itemsSchema))
    }
}
