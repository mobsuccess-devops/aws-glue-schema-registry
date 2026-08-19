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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters.TypeConverterFactory
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException

/**
 * Utilities for mapping between JSON Node to Connect Value.
 */
class JsonNodeToConnectValueConverter(
    private val jsonSchemaDataConfig: JsonSchemaDataConfig,
) {
    private val typeConverterFactory = TypeConverterFactory()

    /**
     * Convert the given JsonNode into a Connect value object.
     */
    fun toConnectValue(
        schema: Schema?,
        jsonValue: JsonNode?,
    ): Any? {
        if (schema == null) {
            return null
        }

        val schemaType = schema.type()
        if (jsonValue == null || jsonValue.isNull) {
            // Any logical type conversions should already have been applied to the default value.
            schema.defaultValue()?.let { return it }
            if (schema.isOptional) {
                return null
            }
            throw DataException("Invalid null value for required $schemaType field")
        }

        if (schema.name() != null) {
            typeConverterFactory.get(schema.name())?.let {
                return it.toConnect(schema, jsonValue, jsonSchemaDataConfig)
            }
        }

        return typeConverterFactory.get(schemaType).toConnect(schema, jsonValue, jsonSchemaDataConfig)
    }
}
