package com.amazonaws.services.schemaregistry.serializers

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.UUID

/**
 * Glue Schema Registry Serializer to be used with Kafka Producers.
 */
// `open`: the test suites mock this type.
open class GlueSchemaRegistryKafkaSerializer(
    credentialProvider: AwsCredentialsProvider?,
    val schemaVersionId: UUID?,
    configs: Map<String, *>?,
) : Serializer<Any> {
    val credentialProvider: AwsCredentialsProvider =
        credentialProvider ?: DefaultCredentialsProvider.builder().build()

    open var dataFormat: String? = null
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

        dataFormat = GlueSchemaRegistryUtils.getInstance().getDataFormat(configs)

        if (schemaName == null) {
            schemaNamingStrategy = GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs)
        }

        if (glueSchemaRegistrySerializationFacade == null) {
            val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
            if (userAgentApp == null) {
                // Set it to kafka if not set by upstream serializers / deserializers
                userAgentApp = UserAgents.KAFKA
            }
            glueSchemaRegistryConfiguration.userAgentApp = userAgentApp
            glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade
                    .builder()
                    .glueSchemaRegistryConfiguration(glueSchemaRegistryConfiguration)
                    .credentialProvider(credentialProvider)
                    .build()
        }
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
        return glueSchemaRegistrySerializationFacade!!.serialize(
            DataFormat.fromValue(dataFormat),
            data,
            schemaVersionIdFromRegistry,
        )
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
    ): AWSSerializerInput {
        val schemaDefinition =
            glueSchemaRegistrySerializationFacade!!.getSchemaDefinition(DataFormat.fromValue(dataFormat), data)

        return AWSSerializerInput
            .builder()
            .schemaDefinition(schemaDefinition)
            .schemaName(getSchemaName(topic, data, isKey))
            .transportName(topic)
            .dataFormat(dataFormat)
            .build()
    }

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryKafkaSerializer::class.java)
    }
}
