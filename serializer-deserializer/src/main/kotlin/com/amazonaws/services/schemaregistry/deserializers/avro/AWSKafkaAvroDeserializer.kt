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

package com.amazonaws.services.schemaregistry.deserializers.avro

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser
import com.amazonaws.services.schemaregistry.deserializers.SecondaryDeserializer
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.serialization.Deserializer
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import java.nio.ByteBuffer

/**
 * AWS Kafka Avro Deserializer responsible for de-serializing the data using
 * Avro protocol serializer.
 */
// `open`: the test suites mock this type, and Kotlin accessors are final unless opened too.
open class AWSKafkaAvroDeserializer(
    val credentialProvider: AwsCredentialsProvider?,
    configs: Map<String, *>?,
) : Deserializer<Any> {
    open var glueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade? = null
    open var userAgentApp: String? = null

    private val secondaryDeserializer: SecondaryDeserializer = SecondaryDeserializer.newInstance()

    init {
        if (configs != null) {
            configure(configs, false)
        }
    }

    /**
     * Constructor used by Kafka consumer.
     */
    constructor() : this(DefaultCredentialsProvider.builder().build(), null)

    constructor(configs: Map<String, *>) : this(DefaultCredentialsProvider.builder().build(), configs)

    /**
     * Configuration method for injecting configuration properties.
     */
    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        if (userAgentApp == null) {
            userAgentApp = UserAgents.KAFKA
        }
        val glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        glueSchemaRegistryConfiguration.userAgentApp = userAgentApp!!
        glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade(glueSchemaRegistryConfiguration, credentialProvider!!)

        if (configs.containsKey(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER)) {
            configureSecondaryDeser(configs, isKey)
        }
    }

    /**
     * De-serialize operation for de-serializing the byte array to an Object.
     */
    override fun deserialize(
        topic: String?,
        data: ByteArray?,
    ): Any? {
        if (data == null) {
            return null
        }
        return deserializeByHeaderVersionByte(topic, data, getHeaderVersionByte(data))
    }

    /**
     * Resource clean up for Closeable.
     */
    override fun close() {
        glueSchemaRegistryDeserializationFacade!!.close()
    }

    private fun prepareInput(
        data: ByteArray,
        topic: String?,
    ): AWSDeserializerInput =
        AWSDeserializerInput.builder().buffer(ByteBuffer.wrap(data)).transportName(topic).build()

    /**
     * Configure the secondary de-serializer and validate that it comes from Kafka.
     */
    private fun configureSecondaryDeser(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        if (!secondaryDeserializer.validate(configs)) {
            throw AWSSchemaRegistryException("The secondary deserializer is not from Kafka")
        }
        secondaryDeserializer.configure(configs, isKey)
    }

    /**
     * De-serialize operation depending on the value of the header version byte.
     */
    private fun deserializeByHeaderVersionByte(
        topic: String?,
        data: ByteArray,
        headerVersionByte: Byte,
    ): Any =
        if (headerVersionByte == AWSSchemaRegistryConstants.HEADER_VERSION_BYTE) {
            glueSchemaRegistryDeserializationFacade!!.deserialize(prepareInput(data, topic))
        } else {
            secondaryDeserializer.deserialize(topic, data)
        }

    private fun getHeaderVersionByte(data: ByteArray): Byte =
        GlueSchemaRegistryDeserializerDataParser.getInstance().getHeaderVersionByte(ByteBuffer.wrap(data))
}
