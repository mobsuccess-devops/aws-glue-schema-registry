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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverterFactory
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException

/**
 * Utilities for mapping between our runtime Connect Value to JSON Node.
 */
class ConnectValueToJsonNodeConverter(
    private val jsonSchemaDataConfig: JsonSchemaDataConfig,
) {
    private val typeConverterFactory = TypeConverterFactory()

    /**
     * Converts a Connect value to a JsonNode.
     *
     * Kafka's own convertToJson is private, so it is reproduced here in simplified form, with the
     * data conversion moved into the TypeConverters.
     */
    fun convertToJson(
        schema: Schema?,
        value: Any?,
    ): JsonNode {
        if (value == null) {
            // Any schema is valid and we don't have a default, so treat this as an optional schema
            if (schema == null) {
                return JSON_NODE_FACTORY.nullNode()
            }
            if (schema.defaultValue() != null) {
                return convertToJson(schema, schema.defaultValue())
            }
            if (schema.isOptional) {
                return JSON_NODE_FACTORY.nullNode()
            }
            throw DataException("Conversion error: null value for field that is required and has no default value")
        }

        if (schema?.name() != null) {
            typeConverterFactory.get(schema.name())?.let {
                return it.toJson(schema, value, jsonSchemaDataConfig)
            }
        }

        try {
            val schemaType =
                if (schema == null) {
                    ConnectSchema.schemaType(value.javaClass)
                        ?: throw DataException("Java class ${value.javaClass} does not have corresponding schema type.")
                } else {
                    schema.type()
                }

            return typeConverterFactory.get(schemaType).toJson(schema, value, jsonSchemaDataConfig)
        } catch (e: ClassCastException) {
            val schemaTypeStr = schema?.type()?.toString() ?: "unknown schema"
            throw DataException("Invalid type for $schemaTypeStr: ${value.javaClass}")
        }
    }

    companion object {
        private val JSON_NODE_FACTORY = TypeConverter.JSON_NODE_FACTORY
    }
}
