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

package com.amazonaws.services.schemaregistry.serializers

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryGlueClientRetryPolicyHelper
import com.amazonaws.services.schemaregistry.common.AWSSerializerInput
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.Properties
import java.util.UUID

// `open`: the test suites mock this type.
open class GlueSchemaRegistrySerializationFacade(
    credentialProvider: AwsCredentialsProvider,
    schemaByDefinitionFetcher: SchemaByDefinitionFetcher?,
    glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration?,
    configs: Map<String, *>?,
    properties: Properties?,
) {
    private val glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration =
        when {
            glueSchemaRegistryConfiguration != null -> glueSchemaRegistryConfiguration
            configs != null -> GlueSchemaRegistryConfiguration(configs)
            properties != null -> GlueSchemaRegistryConfiguration(properties)
            else -> throw AWSSchemaRegistryException("Configuration map and properties cannot be null")
        }

    private val schemaByDefinitionFetcher: SchemaByDefinitionFetcher =
        schemaByDefinitionFetcher ?: SchemaByDefinitionFetcher(
            AWSSchemaRegistryClient(
                credentialProvider,
                this.glueSchemaRegistryConfiguration,
                AWSSchemaRegistryGlueClientRetryPolicyHelper.getRetryPolicy(),
            ),
            this.glueSchemaRegistryConfiguration,
        )

    private val serializationDataEncoder = SerializationDataEncoder(this.glueSchemaRegistryConfiguration)

    private val glueSchemaRegistrySerializerFactory = GlueSchemaRegistrySerializerFactory()

    open fun getOrRegisterSchemaVersion(serializerInput: AWSSerializerInput): UUID =
        schemaByDefinitionFetcher.getORRegisterSchemaVersionId(
            serializerInput.schemaDefinition!!,
            serializerInput.schemaName!!,
            serializerInput.dataFormat!!,
            constructSchemaVersionMetadata(serializerInput.transportName),
        )

    private fun constructSchemaVersionMetadata(transportName: String?): Map<String, String> {
        val metadata = HashMap<String, String>()
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = transportName!!
        glueSchemaRegistryConfiguration.metadata?.let { metadata.putAll(it) }
        return metadata
    }

    open fun serialize(
        dataFormat: DataFormat,
        data: Any,
        schemaVersionId: UUID,
    ): ByteArray {
        val dataFormatSerializer =
            glueSchemaRegistrySerializerFactory.getInstance(dataFormat, glueSchemaRegistryConfiguration)
        return serializationDataEncoder.write(dataFormatSerializer.serialize(data), schemaVersionId)
    }

    open fun encode(
        transportName: String?,
        schema: Schema,
        data: ByteArray,
    ): ByteArray {
        val dataFormat = schema.dataFormat
        val schemaDefinition = schema.schemaDefinition

        val dataFormatSerializer =
            glueSchemaRegistrySerializerFactory.getInstance(
                DataFormat.valueOf(dataFormat),
                glueSchemaRegistryConfiguration,
            )
        // Ensures the data bytes conform to schema definition for data formats like JSON.
        dataFormatSerializer.validate(schemaDefinition, data)

        val schemaVersionId =
            getOrRegisterSchemaVersion(
                AWSSerializerInput
                    .builder()
                    .schemaDefinition(schemaDefinition)
                    .schemaName(schema.schemaName)
                    .dataFormat(dataFormat)
                    .transportName(transportName)
                    .build(),
            )

        return serializationDataEncoder.write(data, schemaVersionId)
    }

    open fun getSchemaDefinition(
        dataFormat: DataFormat,
        data: Any,
    ): String =
        glueSchemaRegistrySerializerFactory
            .getInstance(dataFormat, glueSchemaRegistryConfiguration)
            .getSchemaDefinition(data)

    /** Mirrors the fluent API Lombok generated: called from Java code. */
    class GlueSchemaRegistrySerializationFacadeBuilder internal constructor() {
        private var credentialProvider: AwsCredentialsProvider? = null
        private var schemaByDefinitionFetcher: SchemaByDefinitionFetcher? = null
        private var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration? = null
        private var configs: Map<String, *>? = null
        private var properties: Properties? = null

        fun credentialProvider(credentialProvider: AwsCredentialsProvider?) =
            apply { this.credentialProvider = credentialProvider }

        fun schemaByDefinitionFetcher(schemaByDefinitionFetcher: SchemaByDefinitionFetcher?) =
            apply { this.schemaByDefinitionFetcher = schemaByDefinitionFetcher }

        fun glueSchemaRegistryConfiguration(glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration?) =
            apply { this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration }

        fun configs(configs: Map<String, *>?) = apply { this.configs = configs }

        fun properties(properties: Properties?) = apply { this.properties = properties }

        fun build(): GlueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade(
                credentialProvider!!,
                schemaByDefinitionFetcher,
                glueSchemaRegistryConfiguration,
                configs,
                properties,
            )
    }

    companion object {
        @JvmStatic
        fun builder(): GlueSchemaRegistrySerializationFacadeBuilder = GlueSchemaRegistrySerializationFacadeBuilder()
    }
}
