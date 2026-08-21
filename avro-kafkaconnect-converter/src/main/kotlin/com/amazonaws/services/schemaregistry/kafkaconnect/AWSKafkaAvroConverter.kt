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

package com.amazonaws.services.schemaregistry.kafkaconnect

import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.storage.Converter
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider

/**
 * Amazon Schema Registry Avro converter for Kafka Connect users.
 */
// `open`: the test suites mock this type.
open class AWSKafkaAvroConverter(
    var serializer: AWSKafkaAvroSerializer,
    var deserializer: AWSKafkaAvroDeserializer,
    var avroData: AvroData?,
) : Converter {
    var isKey: Boolean = false

    @VisibleForTesting
    internal val parsedSchemaCache: Cache<String, org.apache.avro.Schema> =
        CacheBuilder
            .newBuilder()
            .maximumSize(MAX_PARSED_SCHEMA_CACHE_SIZE)
            .build()

    /**
     * Constructor used by Kafka Connect user.
     */
    constructor() : this(
        AWSKafkaAvroSerializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
        AWSKafkaAvroDeserializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
        null,
    )

    override fun config(): ConfigDef = AWSKafkaAvroConverterConfig.configDef()

    /**
     * Configure the AWS Avro Converter.
     */
    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        this.isKey = isKey
        AWSKafkaAvroConverterConfig(configs)
        val resolvedConfigs = AWSKafkaAvroConverterConfig.coerce(configs)

        // TODO: add this feature to all other converters
        val roleToAssume = resolvedConfigs[AWSSchemaRegistryConstants.ASSUME_ROLE_ARN] as String?
        if (!roleToAssume.isNullOrEmpty()) {
            val sessionName =
                resolvedConfigs[AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME]?.toString()
                    ?: AWSKafkaAvroConverterConfig.ASSUME_ROLE_SESSION_NAME_DEFAULT
            val region = resolvedConfigs[AWSSchemaRegistryConstants.AWS_REGION].toString()

            val credentialsProvider = getCredentialsProvider(roleToAssume, sessionName, region)

            deserializer = AWSKafkaAvroDeserializer(credentialsProvider, resolvedConfigs)
            serializer = AWSKafkaAvroSerializer(credentialsProvider, resolvedConfigs)
        }

        serializer.configure(resolvedConfigs, this.isKey)
        deserializer.configure(resolvedConfigs, this.isKey)

        avroData = AvroData(AvroDataConfig(resolvedConfigs))
    }

    /**
     * Convert original Connect data to an AVRO serialized byte array.
     */
    override fun fromConnectData(
        topic: String?,
        schema: Schema?,
        value: Any?,
    ): ByteArray? {
        try {
            return serializer.serialize(topic, configuredAvroData().fromConnectData(schema, value))
        } catch (e: SerializationException) {
            throw DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e)
        } catch (e: AWSSchemaRegistryException) {
            throw DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e)
        }
    }

    /**
     * Convert an AVRO serialized byte array to Connect schema and data.
     */
    override fun toConnectData(
        topic: String?,
        value: ByteArray?,
    ): SchemaAndValue? {
        if (value == null) {
            return SchemaAndValue.NULL
        }

        val deserialized =
            try {
                deserializer.deserialize(topic, value)
            } catch (e: SerializationException) {
                throw DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e)
            } catch (e: AWSSchemaRegistryException) {
                throw DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e)
            }

        return configuredAvroData().toConnectData(extractAvroSchema(value, deserialized), deserialized)
    }

    /**
     * Extracts the Avro schema from either GSR metadata or the deserialized Avro object. For GSR
     * data the schema comes from registry metadata; for secondary deserializer data it comes from
     * the Avro object itself.
     */
    @VisibleForTesting
    internal open fun extractAvroSchema(
        value: ByteArray,
        deserialized: Any?,
    ): org.apache.avro.Schema {
        // Check if this is GSR data that can be processed by the GSR deserialization facade.
        val facade = deserializer.glueSchemaRegistryDeserializationFacade
        if (facade != null && facade.canDeserialize(value)) {
            try {
                val schemaDefinition =
                    facade.getSchemaDefinition(value)
                parsedSchemaCache.getIfPresent(schemaDefinition)?.let { return it }
                val parsed =
                    org.apache.avro.Schema
                        .Parser()
                        .parse(schemaDefinition)
                parsedSchemaCache.put(schemaDefinition, parsed)
                return parsed
            } catch (e: Exception) {
                throw DataException("Failed to extract schema from GSR metadata", e)
            }
        }
        return extractSchemaFromAvroObject(deserialized)
    }

    /**
     * Extracts the Avro schema from a deserialized Avro object, supporting both GenericRecord and
     * SpecificRecord.
     */
    @VisibleForTesting
    internal open fun extractSchemaFromAvroObject(avroObject: Any?): org.apache.avro.Schema = when (avroObject) {
        is org.apache.avro.generic.GenericRecord -> avroObject.schema
        is org.apache.avro.specific.SpecificRecord -> avroObject.schema
        else -> throw DataException(
            "Deserialized object is not a valid Avro record. Expected GenericRecord or SpecificRecord, got: " +
                (avroObject?.javaClass?.name ?: "null"),
        )
    }

    @VisibleForTesting
    internal open fun getCredentialsProvider(
        roleArn: String,
        sessionName: String,
        region: String,
    ): AwsCredentialsProvider {
        val stsClient =
            StsClient
                .builder()
                .httpClient(UrlConnectionHttpClient.builder().build())
                .region(Region.of(region))
                .build()
        return StsAssumeRoleCredentialsProvider
            .builder()
            .refreshRequest { assumeRoleRequest ->
                assumeRoleRequest.roleArn(roleArn).roleSessionName(sessionName)
            }.stsClient(stsClient)
            .build()
    }

    private fun configuredAvroData(): AvroData = checkNotNull(avroData) { NOT_CONFIGURED }

    private companion object {
        private const val MAX_PARSED_SCHEMA_CACHE_SIZE = 100L

        private const val NOT_CONFIGURED =
            "configure() has not been called, so this converter is not ready to convert anything"
    }
}
