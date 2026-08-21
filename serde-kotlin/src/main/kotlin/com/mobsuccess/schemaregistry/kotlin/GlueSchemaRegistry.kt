/*
 * Copyright 2026 Mobsuccess.
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

@file:JvmName("GlueSchemaRegistry")

package com.mobsuccess.schemaregistry.kotlin

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.kafkastreams.GlueSchemaRegistryKafkaStreamsSerde
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import org.apache.kafka.common.serialization.Serde

/**
 * The properties described by [configure], as the serializers, deserializers and converters
 * read them.
 *
 * ```
 * val properties = glueSchemaRegistryConfig {
 *     region = "eu-west-1"
 *     dataFormat = DataFormat.AVRO
 *     autoRegistration = true
 * }
 * ```
 */
public fun glueSchemaRegistryConfig(configure: GlueSchemaRegistryConfigBuilder.() -> Unit): Map<String, Any> = GlueSchemaRegistryConfigBuilder().apply(configure).build()

/**
 * A [GlueSchemaRegistryConfiguration] built from the properties described by [configure].
 */
public fun glueSchemaRegistryConfiguration(
    configure: GlueSchemaRegistryConfigBuilder.() -> Unit,
): GlueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(glueSchemaRegistryConfig(configure))

/**
 * A [GlueSchemaRegistryKafkaSerializer] already configured from [configure].
 *
 * ```
 * val serializer = glueSchemaRegistrySerializer {
 *     region = "eu-west-1"
 *     dataFormat = DataFormat.AVRO
 *     autoRegistration = true
 * }
 * ```
 */
public fun glueSchemaRegistrySerializer(
    isKey: Boolean = false,
    configure: GlueSchemaRegistryConfigBuilder.() -> Unit,
): GlueSchemaRegistryKafkaSerializer = GlueSchemaRegistryKafkaSerializer().apply {
    configure(glueSchemaRegistryConfig(configure), isKey)
}

/**
 * A [GlueSchemaRegistryKafkaDeserializer] already configured from [configure].
 */
public fun glueSchemaRegistryDeserializer(
    isKey: Boolean = false,
    configure: GlueSchemaRegistryConfigBuilder.() -> Unit,
): GlueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer().apply {
    configure(glueSchemaRegistryConfig(configure), isKey)
}

/**
 * A `Serde<T>` over the Glue Schema Registry Kafka Streams serde, configured from [configure].
 *
 * ```
 * val serde = glueSchemaRegistrySerde<User> {
 *     region = "eu-west-1"
 *     dataFormat = DataFormat.AVRO
 *     avroRecordType = AvroRecordType.SPECIFIC_RECORD
 * }
 * ```
 *
 * The registry decides at runtime what a record deserializes to, so the type argument is a
 * claim about the topic rather than something the compiler can check. It is checked on every
 * record instead: a value of another type raises a [org.apache.kafka.common.errors.SerializationException]
 * naming both types, where an unchecked cast would have failed somewhere else entirely.
 */
public inline fun <reified T : Any> glueSchemaRegistrySerde(
    isKey: Boolean = false,
    noinline configure: GlueSchemaRegistryConfigBuilder.() -> Unit,
): Serde<T> = glueSchemaRegistrySerde(T::class.java, isKey, configure)

/**
 * A `Serde<T>` over the Glue Schema Registry Kafka Streams serde, for a type known at runtime.
 */
public fun <T : Any> glueSchemaRegistrySerde(
    type: Class<T>,
    isKey: Boolean = false,
    configure: GlueSchemaRegistryConfigBuilder.() -> Unit,
): Serde<T> {
    val serde = GlueSchemaRegistryKafkaStreamsSerde()
    serde.configure(glueSchemaRegistryConfig(configure), isKey)
    return TypedSerde(type, serde)
}

/**
 * This serde seen as a `Serde<T>`, checking the type of every record it reads.
 */
public inline fun <reified T : Any> Serde<Any>.typed(): Serde<T> = typed(T::class.java)

/**
 * This serde seen as a `Serde<T>`, for a type known at runtime.
 */
public fun <T : Any> Serde<Any>.typed(type: Class<T>): Serde<T> = TypedSerde(type, this)
