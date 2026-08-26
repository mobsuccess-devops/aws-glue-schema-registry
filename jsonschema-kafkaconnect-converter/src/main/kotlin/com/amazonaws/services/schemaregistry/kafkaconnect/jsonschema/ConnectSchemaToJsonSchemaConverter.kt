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
import org.apache.commons.collections.MapUtils
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.data.Schema
import org.everit.json.schema.CombinedSchema
import org.everit.json.schema.NullSchema
import java.util.Collections

/**
 * Utilities for mapping between our runtime Connect Schema to JSON Schema.
 */
class ConnectSchemaToJsonSchemaConverter(
    private val jsonSchemaDataConfig: JsonSchemaDataConfig,
) {
    private val connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(jsonSchemaDataConfig)
    private val typeConverterFactory = TypeConverterFactory()

    val fromConnectSchemaCache: Cache<Triple<Schema, Boolean, Int>, org.everit.json.schema.Schema> =
        SynchronizedCache(LRUCache(jsonSchemaDataConfig.getSchemasCacheSize()))
    private val connectMetaData: Boolean = jsonSchemaDataConfig.isConnectMetaData()

    /**
     * Converts a Connect Schema to a Json Schema.
     */
    @JvmOverloads
    fun fromConnectSchema(
        schema: Schema?,
        ignoreOptional: Boolean = false,
        index: Int = -1,
    ): org.everit.json.schema.Schema {
        if (schema == null) {
            return NullSchema.INSTANCE
        }

        val cacheKey = Triple(schema, ignoreOptional, index)
        fromConnectSchemaCache.get(cacheKey)?.let { return it }

        val unprocessedProperties = HashMap<String, Any>()

        val baseSchemaBuilder =
            typeConverterFactory.get(schema).toJsonSchema(schema, unprocessedProperties, jsonSchemaDataConfig)

        // Handles the ordering indicator index so that ordering of Connect Schema is preserved
        if (index != -1) {
            unprocessedProperties[JsonSchemaConverterConstants.CONNECT_INDEX_PROP] = index
        }

        var finalSchema: org.everit.json.schema.Schema =
            baseSchemaBuilder.unprocessedProperties(unprocessedProperties).build()

        if (baseSchemaBuilder !is CombinedSchema.Builder) {
            finalSchema = processNonUnionSchema(baseSchemaBuilder, schema, ignoreOptional, unprocessedProperties, index)
        }

        fromConnectSchemaCache.put(cacheKey, finalSchema)
        return finalSchema
    }

    private fun processNonUnionSchema(
        baseSchemaBuilder: org.everit.json.schema.Schema.Builder<*>,
        schema: Schema,
        ignoreOptional: Boolean,
        unprocessedProperties: MutableMap<String, Any>,
        index: Int,
    ): org.everit.json.schema.Schema {
        if (connectMetaData) {
            populateConnectMetadata(baseSchemaBuilder, schema, unprocessedProperties)
        }

        val shouldBuildOptionalSchema = !ignoreOptional && schema.isOptional

        return if (shouldBuildOptionalSchema) {
            buildOptionalSchema(baseSchemaBuilder, unprocessedProperties, index)
        } else {
            baseSchemaBuilder.unprocessedProperties(unprocessedProperties).build()
        }
    }

    private fun populateConnectMetadata(
        baseSchemaBuilder: org.everit.json.schema.Schema.Builder<*>,
        schema: Schema,
        unprocessedProperties: MutableMap<String, Any>,
    ) {
        addNonEmptyProperties(unprocessedProperties, JsonSchemaConverterConstants.CONNECT_DOC_PROP, schema.doc())
        addNonEmptyProperties(
            unprocessedProperties,
            JsonSchemaConverterConstants.CONNECT_VERSION_PROP,
            schema.version(),
        )
        addNonEmptyProperties(unprocessedProperties, JsonSchemaConverterConstants.CONNECT_NAME_PROP, schema.name())

        val params = parametersFromConnect(schema.parameters())
        if (!MapUtils.isEmpty(params)) {
            unprocessedProperties[JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP] = params!!
        }

        if (schema.defaultValue() != null) {
            addDefaultValue(baseSchemaBuilder, schema)
        }
    }

    private fun buildOptionalSchema(
        baseSchemaBuilder: org.everit.json.schema.Schema.Builder<*>,
        unprocessedProperties: MutableMap<String, Any>,
        index: Int,
    ): org.everit.json.schema.Schema {
        val combinedSchemaBuilder =
            CombinedSchema
                .builder()
                .subschema(baseSchemaBuilder.unprocessedProperties(unprocessedProperties).build())
                .subschema(NullSchema.builder().build())
                .criterion(CombinedSchema.ONE_CRITERION)

        if (index != -1) {
            combinedSchemaBuilder.unprocessedProperties(
                Collections.singletonMap<String, Any>(JsonSchemaConverterConstants.CONNECT_INDEX_PROP, index),
            )
        }

        return combinedSchemaBuilder.build()
    }

    private fun addNonEmptyProperties(
        unprocessedProperties: MutableMap<String, Any>,
        key: String,
        value: Any?,
    ) {
        if (value != null) {
            unprocessedProperties[key] = value
        }
    }

    private fun addDefaultValue(
        baseSchemaBuilder: org.everit.json.schema.Schema.Builder<*>,
        schema: Schema,
    ) {
        // A bytes schema arrives for Decimal Connect types, and logical types come with a schema name
        // property, so for neither of those is the default value set as a primitive.
        val isDefaultValuePrimitive = schema.name() == null && Schema.BYTES_SCHEMA.type() != schema.type()

        val hasJsonDefaultFlagProperty =
            schema.parameters()?.containsKey(JsonSchemaConverterConstants.JSON_FIELD_DEFAULT_FLAG_PROP) == true

        if (!hasJsonDefaultFlagProperty) {
            if (isDefaultValuePrimitive) {
                baseSchemaBuilder.defaultValue(schema.defaultValue())
            } else {
                baseSchemaBuilder.defaultValue(defaultValueFromConnect(schema, schema.defaultValue()))
            }
        }
    }

    private fun defaultValueFromConnect(
        schema: Schema,
        value: Any?,
    ): JsonNode = connectValueToJsonNodeConverter.convertToJson(schema, value)

    companion object {
        private fun parametersFromConnect(params: Map<String, String>?): Map<String, String>? {
            if (params == null) {
                return null
            }
            return params.filterKeys {
                it != JsonSchemaConverterConstants.JSON_FIELD_DEFAULT_FLAG_PROP &&
                    !it.startsWith(JsonSchemaConverterConstants.NAMESPACE)
            }
        }
    }
}
