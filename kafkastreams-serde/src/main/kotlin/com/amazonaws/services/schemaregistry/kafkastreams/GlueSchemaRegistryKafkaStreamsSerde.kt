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

package com.amazonaws.services.schemaregistry.kafkastreams

import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer

/**
 * Amazon Glue Schema Registry serializer and de-serializer wrapper for Kafka Streams users.
 */
class GlueSchemaRegistryKafkaStreamsSerde : Serde<Any> {
    private val inner: Serde<Any>

    /**
     * Constructor used by Kafka Streams user.
     */
    constructor() {
        val glueSchemaRegistryKafkaSerializer = GlueSchemaRegistryKafkaSerializer()
        glueSchemaRegistryKafkaSerializer.setUserAgentApp(UserAgents.KAFKASTREAMS)

        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()
        glueSchemaRegistryKafkaDeserializer.setUserAgentApp(UserAgents.KAFKASTREAMS)

        inner = Serdes.serdeFrom(glueSchemaRegistryKafkaSerializer, glueSchemaRegistryKafkaDeserializer)
    }

    constructor(
        glueSchemaRegistryKafkaSerializer: GlueSchemaRegistryKafkaSerializer,
        glueSchemaRegistryKafkaDeserializer: GlueSchemaRegistryKafkaDeserializer,
    ) {
        inner = Serdes.serdeFrom(glueSchemaRegistryKafkaSerializer, glueSchemaRegistryKafkaDeserializer)
    }

    /**
     * Get the serializer.
     * @return an GlueSchemaRegistryKafkaSerializer instance.
     */
    override fun serializer(): Serializer<Any> = inner.serializer()

    /**
     * Get the de-serializer.
     * @return an GlueSchemaRegistryKafkaDeserializer instance.
     */
    override fun deserializer(): Deserializer<Any> = inner.deserializer()

    /**
     * Configure the serializer and de-serializer wrapper.
     * @param serdeConfig configuration elements for the wrapper
     * @param isSerdeForRecordKeys true if key, false otherwise
     */
    override fun configure(serdeConfig: Map<String, *>, isSerdeForRecordKeys: Boolean) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys)
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys)
    }

    /**
     * Resource clean up for Closeable.
     */
    override fun close() {
        inner.serializer().close()
        inner.deserializer().close()
    }
}
