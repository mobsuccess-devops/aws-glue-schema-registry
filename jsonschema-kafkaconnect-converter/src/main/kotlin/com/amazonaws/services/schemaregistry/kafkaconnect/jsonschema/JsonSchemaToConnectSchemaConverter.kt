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
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException
import org.everit.json.schema.CombinedSchema
import org.everit.json.schema.NullSchema
import org.everit.json.schema.ReferenceSchema

/**
 * Utilities for mapping between JSON Schema to Connect Schema.
 */
class JsonSchemaToConnectSchemaConverter(
    private val jsonSchemaDataConfig: JsonSchemaDataConfig,
) {
    val toConnectSchemaCache: Cache<org.everit.json.schema.Schema, Schema> =
        SynchronizedCache(LRUCache(jsonSchemaDataConfig.getSchemasCacheSize()))
    private val connectMetaData: Boolean = jsonSchemaDataConfig.isConnectMetaData()
    private val typeConverterFactory = TypeConverterFactory()

    /**
     * Convert the given JsonSchema into a Connect Schema object.
     */
    @JvmOverloads
    fun toConnectSchema(
        jsonSchema: org.everit.json.schema.Schema?,
        required: Boolean = true,
    ): Schema? {
        if (jsonSchema == null || NullSchema.INSTANCE == jsonSchema) {
            return null
        }

        toConnectSchemaCache.get(jsonSchema)?.let { return it }

        val connectType =
            jsonSchema.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_TYPE_PROP] as String?
        val connectName =
            jsonSchema.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_NAME_PROP] as String?

        val typeConverter = typeConverterFactory.get(jsonSchema, connectType)

        val builder: SchemaBuilder
        if (typeConverter != null) {
            builder = typeConverter.toConnectSchema(jsonSchema, jsonSchemaDataConfig)
        } else if (jsonSchema is CombinedSchema) {
            val subSchemas = jsonSchema.subschemas
            val hasNullSchema = subSchemas.any { it is NullSchema }

            val criterion = jsonSchema.criterion
            val isNullableUnion =
                hasNullSchema &&
                    (CombinedSchema.ONE_CRITERION == criterion || CombinedSchema.ANY_CRITERION == criterion)
            if (isNullableUnion) {
                return buildOptionalUnionSchema(subSchemas)
            }

            builder = buildNonOptionalUnionSchema(subSchemas, hasNullSchema)
        } else if (jsonSchema is ReferenceSchema) {
            return toConnectSchema(jsonSchema.referredSchema, required)
        } else {
            throw DataException("Unsupported schema type ${jsonSchema.javaClass.name}")
        }

        populateConnectProperties(builder, jsonSchema, required, connectName)

        val result = builder.build()
        toConnectSchemaCache.put(jsonSchema, result)
        return result
    }

    private fun buildOptionalUnionSchema(subSchemas: Collection<org.everit.json.schema.Schema>): Schema? {
        val nonNullSubSchemas = subSchemas.filter { it !is NullSchema }
        if (nonNullSubSchemas.size == 1) {
            return toConnectSchema(nonNullSubSchemas.first(), false)
        }
        return buildNonOptionalUnionSchema(nonNullSubSchemas, true).build()
    }

    private fun buildNonOptionalUnionSchema(
        subSchemas: Collection<org.everit.json.schema.Schema>,
        hasNullSchema: Boolean,
    ): SchemaBuilder {
        val builder = SchemaBuilder.struct().name(JsonSchemaConverterConstants.JSON_SCHEMA_TYPE_ONEOF)

        if (hasNullSchema) {
            builder.optional()
        }

        subSchemas.filter { it !is NullSchema }.forEachIndexed { index, subSchema ->
            builder.field("field${index + 1}", toConnectSchema(subSchema))
        }

        return builder
    }

    private fun populateConnectProperties(
        builder: SchemaBuilder,
        jsonSchema: org.everit.json.schema.Schema,
        required: Boolean,
        connectName: String?,
    ) {
        if (required) {
            builder.required()
        } else {
            builder.optional()
        }

        if (connectName != null) {
            builder.name(connectName)
        }

        val connectDoc = jsonSchema.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_DOC_PROP] as String?
        if (connectDoc != null) {
            builder.doc(connectDoc)
        }

        if (jsonSchema.hasDefaultValue()) {
            builder.defaultValue(jsonSchema.defaultValue)
        }

        val version = jsonSchema.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_VERSION_PROP] as Int?
        if (version != null) {
            builder.version(version)
        }

        @Suppress("UNCHECKED_CAST")
        val parameters =
            jsonSchema.unprocessedProperties[JsonSchemaConverterConstants.CONNECT_PARAMETERS_PROP] as Map<String, String>?
        if (parameters != null) {
            builder.parameters(parameters)
        }
    }
}
