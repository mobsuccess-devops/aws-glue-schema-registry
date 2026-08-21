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

import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.collections.MapUtils
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import java.io.IOException

/**
 * Glue Schema Registry JSON Schema converter for Kafka Connect users.
 */
class JsonSchemaConverter(
    val serializer: GlueSchemaRegistryKafkaSerializer,
    val deserializer: GlueSchemaRegistryKafkaDeserializer,
) : Converter {
    private val objectMapper: ObjectMapper =
        ObjectMapper().setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))

    var connectSchemaToJsonSchemaConverter: ConnectSchemaToJsonSchemaConverter? = null
    var connectValueToJsonNodeConverter: ConnectValueToJsonNodeConverter? = null
    var jsonSchemaToConnectSchemaConverter: JsonSchemaToConnectSchemaConverter? = null
    var jsonNodeToConnectValueConverter: JsonNodeToConnectValueConverter? = null

    var isKey: Boolean = false

    /**
     * Constructor used by Kafka Connect user.
     */
    constructor() : this(
        GlueSchemaRegistryKafkaSerializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
        GlueSchemaRegistryKafkaDeserializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
    )

    override fun config(): ConfigDef = JsonSchemaConverterConfig.configDef()

    /**
     * Configure the JSONSchema Converter.
     */
    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        this.isKey = isKey
        JsonSchemaConverterConfig(configs)
        val resolvedConfigs = JsonSchemaConverterConfig.coerce(configs)

        serializer.configure(resolvedConfigs, this.isKey)
        deserializer.configure(resolvedConfigs, this.isKey)

        if (!MapUtils.isEmpty(resolvedConfigs)) {
            @Suppress("UNCHECKED_CAST")
            val serializationFeatures =
                resolvedConfigs[AWSSchemaRegistryConstants.JACKSON_SERIALIZATION_FEATURES] as List<String>?

            @Suppress("UNCHECKED_CAST")
            val deserializationFeatures =
                resolvedConfigs[AWSSchemaRegistryConstants.JACKSON_DESERIALIZATION_FEATURES] as List<String>?

            if (!CollectionUtils.isEmpty(serializationFeatures)) {
                serializationFeatures!!.forEach { objectMapper.enable(SerializationFeature.valueOf(it)) }
            }
            if (!CollectionUtils.isEmpty(deserializationFeatures)) {
                deserializationFeatures!!.forEach { objectMapper.enable(DeserializationFeature.valueOf(it)) }
            }
        }

        val jsonSchemaDataConfigs = JsonSchemaDataConfig(resolvedConfigs)

        connectSchemaToJsonSchemaConverter = ConnectSchemaToJsonSchemaConverter(jsonSchemaDataConfigs)
        connectValueToJsonNodeConverter = ConnectValueToJsonNodeConverter(jsonSchemaDataConfigs)
        jsonSchemaToConnectSchemaConverter = JsonSchemaToConnectSchemaConverter(jsonSchemaDataConfigs)
        jsonNodeToConnectValueConverter = JsonNodeToConnectValueConverter(jsonSchemaDataConfigs)
    }

    /**
     * Convert original Connect data to a JSON serialized byte array.
     */
    override fun fromConnectData(
        topic: String?,
        schema: Schema?,
        value: Any?,
    ): ByteArray {
        val schemaConverter = checkNotNull(connectSchemaToJsonSchemaConverter) { NOT_CONFIGURED }
        val valueConverter = checkNotNull(connectValueToJsonNodeConverter) { NOT_CONFIGURED }

        try {
            val jsonSchema = schemaConverter.fromConnectSchema(schema)
            val jsonPayload = valueConverter.convertToJson(schema, value).toString()

            val jsonSchemaWithData =
                JsonDataWithSchema.builder(canonicalize(jsonSchema.toString()), jsonPayload).build()
            return serializer.serialize(topic, jsonSchemaWithData)!!
        } catch (e: SerializationException) {
            throw DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e)
        } catch (e: AWSSchemaRegistryException) {
            throw DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e)
        } catch (e: JsonProcessingException) {
            throw DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e)
        }
    }

    /**
     * Convert a JSON serialized byte array to a Connect schema and data.
     */
    override fun toConnectData(
        topic: String?,
        value: ByteArray?,
    ): SchemaAndValue {
        val deserialized =
            try {
                deserializer.deserialize(topic, value)
            } catch (e: SerializationException) {
                throw DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e)
            } catch (e: AWSSchemaRegistryException) {
                throw DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e)
            } ?: return SchemaAndValue.NULL

        val facade =
            checkNotNull(deserializer.glueSchemaRegistryDeserializationFacade) { NOT_CONFIGURED }
        val jsonSchemaString =
            facade.getSchemaDefinition(value!!)

        val jsonSchema =
            try {
                SchemaLoader
                    .builder()
                    .schemaJson(JSONObject(jsonSchemaString))
                    .build()
                    .load()
                    .build()
            } catch (e: Exception) {
                throw DataException("Failed to read JSON Schema : $jsonSchemaString", e)
            }

        if (deserialized !is JsonDataWithSchema) {
            throw DataException("JSON Deserialized data is not in envelope format.")
        }

        val payload = deserialized.payload
        val jsonNode =
            try {
                objectMapper.readTree(payload)
            } catch (e: IOException) {
                throw DataException("Failed to read JSON Payload : $payload", e)
            }

        val connectSchema =
            checkNotNull(jsonSchemaToConnectSchemaConverter) { NOT_CONFIGURED }.toConnectSchema(jsonSchema)
        val connectValue =
            checkNotNull(jsonNodeToConnectValueConverter) { NOT_CONFIGURED }.toConnectValue(connectSchema, jsonNode)

        return SchemaAndValue(connectSchema, connectValue)
    }

    companion object {
        private const val NOT_CONFIGURED =
            "configure() has not been called, so this converter is not ready to convert anything"

        private val CANONICAL_MAPPER =
            ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)

        private val MAP_TYPE = object : TypeReference<Map<String, Any>>() {}

        @JvmStatic
        fun canonicalize(jsonSchema: String): String {
            val properties = CANONICAL_MAPPER.readValue(jsonSchema, MAP_TYPE)
            return CANONICAL_MAPPER.writeValueAsString(properties)
        }
    }
}
