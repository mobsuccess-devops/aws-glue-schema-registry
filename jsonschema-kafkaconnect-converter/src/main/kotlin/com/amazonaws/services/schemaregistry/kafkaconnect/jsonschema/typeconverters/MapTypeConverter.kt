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
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaToConnectSchemaConverter
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException
import org.everit.json.schema.ArraySchema
import org.everit.json.schema.ObjectSchema

class MapTypeConverter : TypeConverter {
    /**
     * A Connect map is represented either as a JSON object or as a JSON array. The array form
     * exists because Connect allows non-string keys while JSON does not; string keys are the
     * default for the JSON representation.
     *
     * When objectMode is true, string keys and a JSON object are used, as in `{"answer": 42}`.
     * When it is false, an array encoding is used, as in `[{"key": "answer", "value": 42}]`.
     */
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode {
        val connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(config)
        val map = value as Map<*, *>

        // With no schema, object mode holds only while every key is a String, exactly as before.
        val objectMode =
            if (schema == null) {
                map.keys.all { it is String }
            } else {
                Schema.Type.STRING == schema.keySchema().type() && !schema.keySchema().isOptional
            }

        val obj = if (objectMode) TypeConverter.JSON_NODE_FACTORY.objectNode() else null
        val list = if (objectMode) null else TypeConverter.JSON_NODE_FACTORY.arrayNode()

        for (entry in map.entries) {
            val mapKey = connectValueToJsonNodeConverter.convertToJson(schema?.keySchema(), entry.key)
            val mapValue = connectValueToJsonNodeConverter.convertToJson(schema?.valueSchema(), entry.value)

            if (objectMode) {
                obj!!.set<JsonNode>(mapKey.asText(), mapValue)
            } else {
                list!!.add(
                    TypeConverter.JSON_NODE_FACTORY.objectNode().setAll<com.fasterxml.jackson.databind.node.ObjectNode>(
                        // A HashMap, as in the original: its iteration order is what the
                        // rendered entry order depends on.
                        HashMap<String, JsonNode>().apply {
                            put(JsonSchemaConverterConstants.KEY_FIELD, mapKey)
                            put(JsonSchemaConverterConstants.VALUE_FIELD, mapValue)
                        },
                    ),
                )
            }
        }
        return if (objectMode) obj!! else list!!
    }

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val connectSchemaToJsonSchemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)
        val keySchema = schema!!.keySchema()

        val baseSchemaBuilder =
            if (Schema.Type.STRING == keySchema.type() && !keySchema.isOptional) {
                ObjectSchema
                    .builder()
                    .schemaOfAdditionalProperties(
                        connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false),
                    )
            } else {
                val mapSchema =
                    ObjectSchema
                        .builder()
                        .addPropertySchema(
                            JsonSchemaConverterConstants.KEY_FIELD,
                            connectSchemaToJsonSchemaConverter.fromConnectSchema(keySchema, false),
                        ).addPropertySchema(
                            JsonSchemaConverterConstants.VALUE_FIELD,
                            connectSchemaToJsonSchemaConverter.fromConnectSchema(schema.valueSchema(), false),
                        ).build()
                ArraySchema.builder().allItemSchema(mapSchema)
            }
        unprocessedProperties[JsonSchemaConverterConstants.CONNECT_TYPE_PROP] = schema.type().getName().lowercase()
        return baseSchemaBuilder
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any {
        val jsonNodeToConnectValueConverter = JsonNodeToConnectValueConverter(jsonSchemaDataConfig)
        val keySchema = schema?.keySchema()
        val valueSchema = schema?.valueSchema()

        // A map keyed by strings is encoded in the natural JSON format. Other primitive or complex
        // key types are encoded as a list of pairs. With no schema, encoding as a map is assumed.
        val result = HashMap<Any?, Any?>()
        if (schema == null || (!keySchema!!.isOptional && keySchema.type() == Schema.Type.STRING)) {
            if (!value.isObject) {
                throw DataException(
                    "Maps with string fields should be encoded as JSON objects, but found ${value.nodeType}",
                )
            }
            val fieldIt = value.fields()
            while (fieldIt.hasNext()) {
                val entry = fieldIt.next()
                result[entry.key] = jsonNodeToConnectValueConverter.toConnectValue(valueSchema, entry.value)
            }
        } else {
            if (!value.isArray) {
                throw DataException(
                    "Maps with non-string fields should be encoded as JSON array of tuples, but found ${value.nodeType}",
                )
            }
            for (entry in value) {
                if (!entry.isObject) {
                    throw DataException("Found invalid map entry instead of object: ${entry.nodeType}")
                }
                if (entry.size() != 2) {
                    throw DataException("Found invalid map entry, expected length 2 but found :${entry.size()}")
                }
                result[
                    jsonNodeToConnectValueConverter.toConnectValue(
                        keySchema,
                        entry.get(JsonSchemaConverterConstants.KEY_FIELD),
                    ),
                ] = jsonNodeToConnectValueConverter.toConnectValue(
                    valueSchema,
                    entry.get(JsonSchemaConverterConstants.VALUE_FIELD),
                )
            }
        }
        return result
    }

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder {
        val converter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig)

        if (jsonSchema is ArraySchema) {
            val itemsSchema =
                jsonSchema.allItemSchema ?: throw DataException("Array schema did not specify the items type")
            val objectSchema = itemsSchema as ObjectSchema
            return SchemaBuilder.map(
                converter.toConnectSchema(objectSchema.propertySchemas[JsonSchemaConverterConstants.KEY_FIELD]),
                converter.toConnectSchema(objectSchema.propertySchemas[JsonSchemaConverterConstants.VALUE_FIELD]),
            )
        }

        if (jsonSchema is ObjectSchema) {
            return SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                converter.toConnectSchema(jsonSchema.schemaOfAdditionalProperties),
            )
        }

        throw DataException("Json Schema for Connect Map translation should be either Object or Array Schema.")
    }
}
