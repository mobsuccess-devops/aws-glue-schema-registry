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

package com.amazonaws.services.schemaregistry.serializers.avro

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.UUID

// `open`: the test suites mock this type, and Kotlin accessors are final unless opened too.
open class AWSKafkaAvroSerializer(
    credentialProvider: AwsCredentialsProvider?,
    val schemaVersionId: UUID?,
    configs: Map<String, *>?,
) : Serializer<Any> {
    val credentialProvider: AwsCredentialsProvider =
        credentialProvider ?: DefaultCredentialsProvider.builder().build()

    open var glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade? = null
    open var schemaName: String? = null
    open var schemaNamingStrategy: AWSSchemaNamingStrategy? = null
    open var isKey: Boolean = false
    open var userAgentApp: String? = null

    init {
        if (configs != null) {
            configure(configs, false)
        }
    }

    /**
     * Constructor used by Kafka producer when passing as the property.
     */
    constructor() : this(DefaultCredentialsProvider.builder().build(), null, null)

    constructor(configs: Map<String, *>?) : this(DefaultCredentialsProvider.builder().build(), null, configs)

    constructor(credentialProvider: AwsCredentialsProvider?, configs: Map<String, *>?) :
        this(credentialProvider, null, configs)

    constructor(configs: Map<String, *>, schemaVersionId: UUID?) :
        this(DefaultCredentialsProvider.builder().build(), schemaVersionId, configs)

    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        schemaName = GlueSchemaRegistryUtils.getInstance().getSchemaName(configs)
        this.isKey = isKey

        if (schemaName == null) {
            schemaNamingStrategy = GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs)
        }
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        if (userAgentApp == null) {
            // Set it to kafka if not set by upstream serializers / deserializers
            userAgentApp = UserAgents.KAFKA
        }
        glueSchemaRegistryConfiguration.userAgentApp = userAgentApp!!
        glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .glueSchemaRegistryConfiguration(glueSchemaRegistryConfiguration)
                .credentialProvider(credentialProvider)
                .build()
    }

    override fun serialize(
        topic: String?,
        data: Any?,
    ): ByteArray? {
        if (data == null) {
            return null
        }

        val schemaVersionIdFromRegistry =
            if (schemaVersionId == null) {
                log.debug("Schema Version Id is null. Trying to register the schema.")
                glueSchemaRegistrySerializationFacade!!.getOrRegisterSchemaVersion(prepareInput(data, topic, isKey))
            } else {
                schemaVersionId
            }

        log.debug("Schema Version Id received from the from schema registry: {}", schemaVersionIdFromRegistry)
        return glueSchemaRegistrySerializationFacade!!.serialize(DATA_FORMAT, data, schemaVersionIdFromRegistry)
    }

    override fun close() {
        // No-op.
    }

    /**
     * Resolves the schema name from, in order: a dynamically configured
     * AWSSchemaNamingStrategy, the schema name given in the configuration, or the one the client
     * generates through AWSSchemaNamingStrategyDefaultImpl.
     */
    private fun getSchemaName(
        topic: String?,
        data: Any,
        isKey: Boolean?,
    ): String? = schemaName ?: schemaNamingStrategy!!.getSchemaName(topic, data, isKey!!)

    // isKey stays boxed: the Java signature used Boolean and the tests look this method up
    // reflectively by that exact signature.
    private fun prepareInput(
        data: Any,
        topic: String?,
        isKey: Boolean?,
    ): AWSSerializerInput =
        AWSSerializerInput
            .builder()
            .schemaDefinition(AVROUtils.getInstance().getSchemaDefinition(data))
            .schemaName(getSchemaName(topic, data, isKey))
            .transportName(topic)
            .dataFormat(DATA_FORMAT.name)
            .build()

    companion object {
        private val log = LoggerFactory.getLogger(AWSKafkaAvroSerializer::class.java)
        private val DATA_FORMAT = DataFormat.AVRO
    }
}
