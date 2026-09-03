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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaClient
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONArray
import org.json.JSONObject
import java.io.InputStream

/**
 * Json validator
 */
class JsonValidator(
    private val mapper: ObjectMapper,
) {
    /**
     * Validates with a mapper of its own, for a caller that has none to share.
     */
    constructor() : this(ObjectMapper())

    /**
     * Validates data against JsonSchema.
     */
    fun validateDataWithSchema(
        schemaNode: JsonNode,
        dataNode: JsonNode,
    ) {
        try {
            val rawSchema = JSONObject(mapper.writeValueAsString(schemaNode))
            val schema = SchemaLoader.load(rawSchema, ReferenceDisabledSchemaClient())

            when (dataNode.nodeType) {
                JsonNodeType.OBJECT, JsonNodeType.POJO ->
                    schema.validate(JSONObject(mapper.writeValueAsString(dataNode)))
                JsonNodeType.ARRAY -> schema.validate(JSONArray(mapper.writeValueAsString(dataNode)))
                JsonNodeType.STRING -> schema.validate(dataNode.textValue())
                JsonNodeType.NUMBER -> schema.validate(dataNode.numberValue())
                JsonNodeType.NULL -> schema.validate(JSONObject.NULL)
                JsonNodeType.BOOLEAN -> schema.validate(dataNode.booleanValue())
                JsonNodeType.BINARY -> schema.validate(dataNode.toString())
                else -> throw AWSSchemaRegistryException(
                    "JsonNodeType is unknown or unsupported: ${dataNode.nodeType}",
                )
            }
        } catch (e: Exception) {
            throw AWSSchemaRegistryException("JSON data validation against schema failed.", e)
        }
    }

    /**
     * The override SchemaClient which disables external schema reference.
     */
    inner class ReferenceDisabledSchemaClient : SchemaClient {
        override fun get(url: String): InputStream = throw ValidationException("Remote or local reference is not allowed: $url")
    }
}
