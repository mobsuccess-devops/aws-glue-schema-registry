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
package com.amazonaws.services.schemaregistry.integrationtests.kafka

import com.amazonaws.services.schemaregistry.integrationtests.properties.GlueSchemaRegistryConnectionProperties

class ProducerProperties private constructor(
    val topicName: String?,
    val schemaName: String?,
    val dataFormat: String?,
    val compatibilityType: String?,
    val compressionType: String?,
    val autoRegistrationEnabled: String?,
    // Streaming properties
    val inputTopic: String?,
    val outputTopic: String?,
    // required only for AVRO or Protobuf case
    val recordType: String?,
    // Kafka's native compression type (e.g., "lz4", "snappy", "gzip", "zstd")
    val kafkaCompressionType: String?,
) : GlueSchemaRegistryConnectionProperties {
    class Builder {
        private var topicName: String? = null
        private var schemaName: String? = null
        private var dataFormat: String? = null
        private var compatibilityType: String? = null
        private var compressionType: String? = null
        private var autoRegistrationEnabled: String? = null
        private var inputTopic: String? = null
        private var outputTopic: String? = null
        private var recordType: String? = null
        private var kafkaCompressionType: String? = null

        fun topicName(topicName: String?): Builder = apply { this.topicName = topicName }

        fun schemaName(schemaName: String?): Builder = apply { this.schemaName = schemaName }

        fun dataFormat(dataFormat: String?): Builder = apply { this.dataFormat = dataFormat }

        fun compatibilityType(compatibilityType: String?): Builder = apply { this.compatibilityType = compatibilityType }

        fun compressionType(compressionType: String?): Builder = apply { this.compressionType = compressionType }

        fun autoRegistrationEnabled(autoRegistrationEnabled: String?): Builder = apply { this.autoRegistrationEnabled = autoRegistrationEnabled }

        fun inputTopic(inputTopic: String?): Builder = apply { this.inputTopic = inputTopic }

        fun outputTopic(outputTopic: String?): Builder = apply { this.outputTopic = outputTopic }

        fun recordType(recordType: String?): Builder = apply { this.recordType = recordType }

        fun kafkaCompressionType(kafkaCompressionType: String?): Builder = apply { this.kafkaCompressionType = kafkaCompressionType }

        fun build(): ProducerProperties = ProducerProperties(
            topicName,
            schemaName,
            dataFormat,
            compatibilityType,
            compressionType,
            autoRegistrationEnabled,
            inputTopic,
            outputTopic,
            recordType,
            kafkaCompressionType,
        )
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
