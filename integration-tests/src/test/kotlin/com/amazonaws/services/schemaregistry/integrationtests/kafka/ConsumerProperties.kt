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

class ConsumerProperties private constructor(
    val topicName: String?,
    val avroRecordType: String?,
    val protobufMessageType: String?,
    // As of 2.0.0, resolving a JSON schema's "className" into a POJO is opt-in and gated behind an
    // allowlist. Consumers expecting a typed POJO back must set both of these.
    val jsonClassNameResolutionEnabled: Boolean?,
    val jsonClassNameAllowlist: String?,
) : GlueSchemaRegistryConnectionProperties {
    class Builder {
        private var topicName: String? = null
        private var avroRecordType: String? = null
        private var protobufMessageType: String? = null
        private var jsonClassNameResolutionEnabled: Boolean? = null
        private var jsonClassNameAllowlist: String? = null

        fun topicName(topicName: String?): Builder = apply { this.topicName = topicName }

        fun avroRecordType(avroRecordType: String?): Builder = apply { this.avroRecordType = avroRecordType }

        fun protobufMessageType(protobufMessageType: String?): Builder = apply { this.protobufMessageType = protobufMessageType }

        fun jsonClassNameResolutionEnabled(jsonClassNameResolutionEnabled: Boolean?): Builder = apply { this.jsonClassNameResolutionEnabled = jsonClassNameResolutionEnabled }

        fun jsonClassNameAllowlist(jsonClassNameAllowlist: String?): Builder = apply { this.jsonClassNameAllowlist = jsonClassNameAllowlist }

        fun build(): ConsumerProperties = ConsumerProperties(
            topicName,
            avroRecordType,
            protobufMessageType,
            jsonClassNameResolutionEnabled,
            jsonClassNameAllowlist,
        )
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
