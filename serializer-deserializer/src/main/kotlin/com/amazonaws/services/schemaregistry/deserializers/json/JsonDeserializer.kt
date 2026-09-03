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

package com.amazonaws.services.schemaregistry.deserializers.json

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.DefaultObjectMapperFactory
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser
import com.amazonaws.services.schemaregistry.deserializers.PojoClassResolver
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Json specific de-serializer responsible for handling the Json data format
 * specific deserialization behavior.
 */
// `open`: the test suites mock this type.
open class JsonDeserializer(
    configs: GlueSchemaRegistryConfiguration?,
) : GlueSchemaRegistryDataFormatDeserializer {
    private val objectMapper: ObjectMapper =
        configs?.buildObjectMapper() ?: DefaultObjectMapperFactory().newObjectMapper()

    // Exposed because @Data generated getters for them, which the tests use; they were only
    // excluded from equals/hashCode/toString.
    /** Class names already warned about, so the allowlist-miss warning is not logged per record. */
    val warnedClassNames: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap())

    /** Guards the one-time notice that warning has stopped, so suppression is never silent. */
    val warnCapNoticeEmitted = AtomicBoolean(false)

    var schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration? = configs

    /**
     * Deserialize the bytes to the original JSON message given the schema retrieved
     * from the schema registry.
     *
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    override fun deserialize(
        data: ByteBuffer,
        schema: Schema,
    ): Any {
        try {
            val plainData = DESERIALIZER_DATA_PARSER.getPlainData(data)

            log.debug("Length of actual message: {}", plainData.size)

            val schemaNode = objectMapper.readTree(schema.schemaDefinition)
            val classNameNode = schemaNode.get("className")

            val configs = schemaRegistrySerDeConfigs
            val classNameResolutionEnabled = configs != null && configs.isJsonClassNameResolutionEnabled

            if (classNameResolutionEnabled && classNameNode != null) {
                val className = classNameNode.asText()
                if (configs!!.isClassNameAllowed(className)) {
                    return objectMapper.readValue(plainData, PojoClassResolver.resolve(className))
                }
                warnOnceForDisallowedClassName(className)
            }

            val dataNode = objectMapper.readTree(plainData)
            return JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString()).build()
        } catch (e: IOException) {
            throw AWSSchemaRegistryException("Exception occurred while de-serializing JSON message.", e)
        } catch (e: ClassNotFoundException) {
            throw AWSSchemaRegistryException("Exception occurred while de-serializing JSON message.", e)
        }
    }

    /**
     * Warns that a schema's className was not allowlisted, normally once per distinct class name.
     * The condition is configuration-scoped rather than record-scoped, so logging it on every
     * record would flood the logs at message throughput rate.
     *
     * Dedup state is capped at [MAX_WARNED_CLASS_NAMES] entries so that a stream of distinct
     * schema-supplied class names cannot grow it without bound. On reaching the cap, warning stops
     * rather than falling back to once per record. Suppression is announced once so that it is not
     * silent; by that point the cap's worth of warnings has already named the problem.
     *
     * The cap is approximate under concurrency, since the size check and the insert are not atomic
     * with respect to each other. It can be exceeded by roughly the number of threads deserializing
     * at once, which does not affect the ceiling in any meaningful way.
     */
    private fun warnOnceForDisallowedClassName(className: String) {
        if (warnedClassNames.size >= MAX_WARNED_CLASS_NAMES) {
            if (warnCapNoticeEmitted.compareAndSet(false, true)) {
                log.warn(
                    "Reached {} distinct class names outside the allowlist; suppressing further warnings. Review {}.",
                    MAX_WARNED_CLASS_NAMES,
                    AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST,
                )
            }
            return
        }
        if (warnedClassNames.add(className)) {
            log.warn(
                "className '{}' is not in the configured allowlist. Returning JsonDataWithSchema instead. " +
                    "Add the class to {} to enable typed deserialization.",
                className,
                AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST,
            )
        }
    }

    // @Data generated equals/hashCode/toString, excluding the two dedup fields.
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is JsonDeserializer) return false
        return objectMapper == other.objectMapper && schemaRegistrySerDeConfigs == other.schemaRegistrySerDeConfigs
    }

    override fun hashCode(): Int = 31 * objectMapper.hashCode() + (schemaRegistrySerDeConfigs?.hashCode() ?: 0)

    override fun toString(): String = "JsonDeserializer(objectMapper=$objectMapper, schemaRegistrySerDeConfigs=$schemaRegistrySerDeConfigs)"

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class JsonDeserializerBuilder internal constructor() {
        private var configs: GlueSchemaRegistryConfiguration? = null

        fun configs(configs: GlueSchemaRegistryConfiguration?): JsonDeserializerBuilder = apply { this.configs = configs }

        fun build(): JsonDeserializer = JsonDeserializer(configs)
    }

    companion object {
        private val log = LoggerFactory.getLogger(JsonDeserializer::class.java)
        private val DESERIALIZER_DATA_PARSER = GlueSchemaRegistryDeserializerDataParser.getInstance()

        /**
         * Upper bound on the warned-class-name set. The dedup key comes from the schema, which a
         * producer controls, so an unbounded set would grow for the lifetime of the deserializer.
         *
         * The count of distinct class names one deserializer can legitimately see is bounded by the
         * distinct schemas across the topics its consumer reads — typically a handful, and at most
         * tens for a large fan-in consumer. This cap therefore sits above any realistic
         * configuration: reaching it means the allowlist is wrong, or a producer is supplying class
         * names in bulk.
         */
        const val MAX_WARNED_CLASS_NAMES = 100

        @JvmStatic
        fun builder(): JsonDeserializerBuilder = JsonDeserializerBuilder()
    }
}
