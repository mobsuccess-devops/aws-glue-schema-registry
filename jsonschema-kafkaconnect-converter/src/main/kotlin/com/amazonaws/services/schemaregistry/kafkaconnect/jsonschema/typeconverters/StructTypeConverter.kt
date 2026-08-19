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
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BigIntegerNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.FloatNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.everit.json.schema.CombinedSchema
import org.everit.json.schema.NullSchema
import org.everit.json.schema.ObjectSchema
import java.math.BigDecimal

class StructTypeConverter : TypeConverter {
    /**
     * A Connect Struct is a structured record of named fields, each with its own Schema. Structs
     * must declare a complete Schema up front, and only the fields it declares may be set. They are
     * represented as a JSON object, as in `{"name": "john", "age": 42}`.
     */
    override fun toJson(
        schema: Schema?,
        value: Any?,
        config: JsonSchemaDataConfig,
    ): JsonNode {
        val connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(config)
        val struct = value as Struct
        if (struct.schema() != schema) {
            throw DataException("Mismatching schema.")
        }

        // Handle JSON union / one-of schemas that do not result from an optional field, such as
        // {"oneOf":[{"type":"integer","connect.type":"int32"},{"type":"string"}]}
        if (JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF == schema!!.name()) {
            for (field in schema.fields()) {
                val obj = struct.get(field)
                if (obj != null) {
                    return connectValueToJsonNodeConverter.convertToJson(field.schema(), obj)
                }
            }
            return connectValueToJsonNodeConverter.convertToJson(schema, null)
        }

        val obj = TypeConverter.JSON_NODE_FACTORY.objectNode()
        for (field in schema.fields()) {
            val fieldValue = struct.get(field)
            if (fieldValue != null) {
                obj.set<JsonNode>(field.name(), connectValueToJsonNodeConverter.convertToJson(field.schema(), fieldValue))
            }
        }
        return obj
    }

    override fun toJsonSchema(
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): org.everit.json.schema.Schema.Builder<*> {
        val converter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfig)

        if (JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF == schema.name()) {
            val unionSchemas = ArrayList<org.everit.json.schema.Schema>()
            if (schema.isOptional) {
                unionSchemas.add(NullSchema.builder().build())
            }
            for (field in schema.fields()) {
                unionSchemas.add(converter.fromConnectSchema(nonOptional(field.schema()), true, field.index()))
            }
            return CombinedSchema.oneOf(unionSchemas)
        }

        if (schema.isOptional) {
            val unionSchemas = ArrayList<org.everit.json.schema.Schema>()
            unionSchemas.add(NullSchema.builder().build())
            unionSchemas.add(converter.fromConnectSchema(nonOptional(schema), false))
            return CombinedSchema.oneOf(unionSchemas)
        }

        val objectSchemaBuilder = ObjectSchema.builder()
        for (field in schema.fields()) {
            objectSchemaBuilder.addPropertySchema(
                field.name(),
                converter.fromConnectSchema(field.schema(), false, field.index()),
            )
        }
        return objectSchemaBuilder
    }

    override fun toConnect(
        schema: Schema?,
        value: JsonNode,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): Any {
        val converter = JsonNodeToConnectValueConverter(jsonSchemaDataConfig)

        if (JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF == schema!!.name()) {
            // Special case support for union types
            for (field in schema.fields()) {
                val fieldSchema = field.schema()
                if (isInstanceOfJsonSchemaTypeForSimpleSchema(fieldSchema, value) ||
                    structSchemaEquals(fieldSchema, value)
                ) {
                    return Struct(schema.schema())
                        .put("field${field.index() + 1}", converter.toConnectValue(fieldSchema, value))
                }
            }
            throw DataException("Did not find matching union field for data: $value")
        }

        if (!value.isObject) {
            throw DataException("Structs should be encoded as JSON objects, but found ${value.nodeType}")
        }

        val result = Struct(schema.schema())
        for (field in schema.fields()) {
            val fieldValue = value.get(field.name())
            if (fieldValue != null) {
                result.put(field, converter.toConnectValue(field.schema(), fieldValue))
            }
        }
        return result
    }

    override fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema,
        jsonSchemaDataConfig: JsonSchemaDataConfig,
    ): SchemaBuilder {
        val converter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfig)
        val builder = SchemaBuilder.struct()

        if (jsonSchema !is ObjectSchema) {
            throw DataException("Non Object Json Schema can not be converted to Connect Struct type Schema.")
        }

        for ((subFieldName, subSchema) in getOrderedFields(jsonSchema)) {
            builder.field(subFieldName, converter.toConnectSchema(subSchema, true))
        }

        return builder
    }

    private fun getOrderedFields(objectSchema: ObjectSchema): Map<String, org.everit.json.schema.Schema> {
        // The connect.index property is absent when the schema was not produced by a source-side
        // converter — a plain producer registering its schema directly with Glue Schema Registry.
        // Entries without an index sort last, keeping the comparison total.
        val indexOf = { s: org.everit.json.schema.Schema ->
            s.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_INDEX_PROP] as Int?
        }
        return objectSchema.propertySchemas.entries
            .sortedWith(
                Comparator { s1, s2 ->
                    val index1 = indexOf(s1.value)
                    val index2 = indexOf(s2.value)
                    when {
                        index1 == null && index2 == null -> 0
                        index1 == null -> 1
                        index2 == null -> -1
                        else -> index1.compareTo(index2)
                    }
                },
            ).associateTo(LinkedHashMap()) { it.key to it.value }
    }

    companion object {
        // Json Java object types used by Connect schema types
        private val SIMPLE_JSON_SCHEMA_TYPES: Map<Schema.Type, List<Class<*>>> =
            mapOf(
                Schema.Type.INT8 to listOf(IntNode::class.java, LongNode::class.java, BigIntegerNode::class.java),
                Schema.Type.INT16 to listOf(IntNode::class.java, LongNode::class.java, BigIntegerNode::class.java),
                Schema.Type.INT32 to listOf(IntNode::class.java, LongNode::class.java, BigIntegerNode::class.java),
                Schema.Type.INT64 to listOf(IntNode::class.java, LongNode::class.java, BigIntegerNode::class.java),
                Schema.Type.FLOAT32 to listOf(FloatNode::class.java, DoubleNode::class.java, DecimalNode::class.java),
                Schema.Type.FLOAT64 to listOf(FloatNode::class.java, DoubleNode::class.java, DecimalNode::class.java),
                Schema.Type.BOOLEAN to listOf(BooleanNode::class.java),
                Schema.Type.STRING to listOf(TextNode::class.java),
                Schema.Type.BYTES to listOf(BinaryNode::class.java, BigDecimal::class.java),
                Schema.Type.ARRAY to listOf(ArrayNode::class.java),
                Schema.Type.MAP to listOf(Map::class.java, ArrayNode::class.java),
            )

        @JvmStatic
        fun nonOptional(schema: Schema): Schema = ConnectSchema(
            schema.type(),
            false,
            schema.defaultValue(),
            schema.name(),
            schema.version(),
            schema.doc(),
            schema.parameters(),
            fields(schema),
            keySchema(schema),
            valueSchema(schema),
        )

        @JvmStatic
        fun fields(schema: Schema): List<Field>? = if (Schema.Type.STRUCT == schema.type()) schema.fields() else null

        @JvmStatic
        fun keySchema(schema: Schema): Schema? = if (Schema.Type.MAP == schema.type()) schema.keySchema() else null

        @JvmStatic
        fun valueSchema(schema: Schema): Schema? = if (Schema.Type.MAP == schema.type() || Schema.Type.ARRAY == schema.type()) schema.valueSchema() else null

        private fun isInstanceOfJsonSchemaTypeForSimpleSchema(
            fieldSchema: Schema,
            value: JsonNode,
        ): Boolean {
            val classes = SIMPLE_JSON_SCHEMA_TYPES[fieldSchema.type()] ?: return false
            for (type in classes) {
                if (type.isInstance(value)) {
                    if (fieldSchema.type() == Schema.Type.MAP && value.isArray) {
                        return isMapAsArray(value)
                    }
                    return true
                }
            }
            return false
        }

        private fun structSchemaEquals(
            fieldSchema: Schema,
            value: JsonNode,
        ): Boolean {
            if (fieldSchema.type() != Schema.Type.STRUCT || !value.isObject) {
                return false
            }
            val schemaFields = fieldSchema.fields().map { it.name() }.toSet()

            val jsonNodeIterator = value.fields()
            while (jsonNodeIterator.hasNext()) {
                if (schemaFields.contains(jsonNodeIterator.next().key)) {
                    return true
                }
            }
            return false
        }

        private fun isMapAsArray(value: JsonNode): Boolean {
            val arrayNode = value as ArrayNode
            for (i in 0 until arrayNode.size()) {
                val currentNode = arrayNode.get(i)
                if (!currentNode.has(JsonSchemaConverterConstants.KEY_FIELD) &&
                    !currentNode.has(JsonSchemaConverterConstants.VALUE_FIELD)
                ) {
                    return false
                }
            }
            return true
        }
    }
}
