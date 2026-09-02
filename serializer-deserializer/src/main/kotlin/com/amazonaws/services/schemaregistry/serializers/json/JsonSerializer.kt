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

package com.amazonaws.services.schemaregistry.serializers.json

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import org.apache.commons.collections4.CollectionUtils
import java.nio.charset.StandardCharsets

/**
 * Json serialization helper.
 */
// `open`: the test suites mock this type.
open class JsonSerializer(
    configs: GlueSchemaRegistryConfiguration?,
) : GlueSchemaRegistryDataFormatSerializer {
    private val objectMapper: ObjectMapper = ObjectMapper()
    private val jsonSchemaGenerator: JsonSchemaGenerator

    var schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration? = configs

    init {
        objectMapper.nodeFactory = JsonNodeFactory.withExactBigDecimals(true)
        if (configs != null) {
            if (!CollectionUtils.isEmpty(configs.jacksonSerializationFeatures)) {
                configs.jacksonSerializationFeatures!!.forEach { objectMapper.enable(it) }
            }
            if (!CollectionUtils.isEmpty(configs.jacksonDeserializationFeatures)) {
                configs.jacksonDeserializationFeatures!!.forEach { objectMapper.enable(it) }
            }
            configs.jacksonSerializationFeatureToggles?.forEach { (feature, enabled) ->
                objectMapper.configure(feature, enabled)
            }
            configs.jacksonDeserializationFeatureToggles?.forEach { (feature, enabled) ->
                objectMapper.configure(feature, enabled)
            }
        }
        jsonSchemaGenerator =
            if (configs != null && configs.isJsonSchemaNullableEnabled) {
                JsonSchemaGenerator(objectMapper, JsonSchemaConfig.nullableJsonSchemaDraft4())
            } else {
                JsonSchemaGenerator(objectMapper)
            }
    }

    /**
     * Serialize the JSON object to bytes.
     *
     * @throws AWSSchemaRegistryException AWS Schema Registry Exception
     */
    override fun serialize(data: Any): ByteArray {
        val dataNode = getDataNode(data)
        val schemaNode = getSchemaNode(data)
        JSON_VALIDATOR.validateDataWithSchema(schemaNode, dataNode)
        return writeBytes(dataNode)
    }

    private fun writeBytes(dataNode: JsonNode): ByteArray = try {
        objectMapper.writeValueAsBytes(dataNode)
    } catch (e: Exception) {
        throw AWSSchemaRegistryException(e.message, e)
    }

    /**
     * Whether an object is of generic json type, i.e. it carries a schema string and a data string.
     */
    private fun isWrapper(objectToCheck: Any): Boolean = objectToCheck is JsonDataWithSchema

    /**
     * Get the schema definition.
     */
    override fun getSchemaDefinition(objectToSerialize: Any): String = getSchemaNode(objectToSerialize).toString()

    private fun getSchemaNode(objectToSerialize: Any): JsonNode {
        if (isWrapper(objectToSerialize)) {
            return getSchemaNodeFromWrapperObject(objectToSerialize as JsonDataWithSchema)
        }
        try {
            return jsonSchemaGenerator.generateJsonSchema(objectToSerialize.javaClass)
        } catch (e: Exception) {
            throw AWSSchemaRegistryException(
                "Could not generate schema from the type provided ${objectToSerialize.javaClass}",
                e,
            )
        }
    }

    private fun getDataNode(objectToSerialize: Any): JsonNode = if (isWrapper(objectToSerialize)) {
        getDataNodeFromWrapperObject(objectToSerialize as JsonDataWithSchema)
    } else {
        getDataNodeFromSpecificObject(objectToSerialize)
    }

    private fun getSchemaNodeFromWrapperObject(objectToSerialize: JsonDataWithSchema): JsonNode = convertToJsonNode(objectToSerialize.schema)

    private fun getDataNodeFromWrapperObject(objectToSerialize: JsonDataWithSchema): JsonNode = convertToJsonNode(objectToSerialize.payload)

    private fun getDataNodeFromSpecificObject(objectToSerialize: Any): JsonNode = try {
        objectMapper.valueToTree(objectToSerialize)
    } catch (e: Exception) {
        throw AWSSchemaRegistryException("Not a valid Specific Json Record.", e)
    }

    private fun convertToJsonNode(jsonString: String?): JsonNode = try {
        objectMapper.readTree(jsonString)
    } catch (e: JsonProcessingException) {
        throw AWSSchemaRegistryException("Malformed JSON", e)
    }

    override fun validate(
        schemaDefinition: String,
        data: ByteArray,
    ) {
        // We assume the data bytes are encoded as UTF-8 Strings.
        // We might want to provide customization of this if required.
        val payload = String(data, StandardCharsets.UTF_8)

        val jsonDataWithSchema =
            JsonDataWithSchema
                .builder()
                .schema(schemaDefinition)
                .payload(payload)
                .build()

        validate(jsonDataWithSchema)
    }

    override fun validate(data: Any) {
        JSON_VALIDATOR.validateDataWithSchema(getSchemaNode(data), getDataNode(data))
    }

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class JsonSerializerBuilder internal constructor() {
        private var configs: GlueSchemaRegistryConfiguration? = null

        fun configs(configs: GlueSchemaRegistryConfiguration?): JsonSerializerBuilder = apply { this.configs = configs }

        fun build(): JsonSerializer = JsonSerializer(configs)
    }

    companion object {
        private val JSON_VALIDATOR = JsonValidator()

        @JvmStatic
        fun builder(): JsonSerializerBuilder = JsonSerializerBuilder()
    }
}
