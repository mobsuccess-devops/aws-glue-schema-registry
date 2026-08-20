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

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.google.common.annotations.VisibleForTesting
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * {@inheritDoc}
 */
class GlueSchemaRegistrySerializerImpl : GlueSchemaRegistrySerializer {
    private val glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade

    /**
     * Initialize an instance of GlueSchemaRegistrySerializer with Region.
     */
    constructor(awsCredentialsProvider: AwsCredentialsProvider, region: String) {
        glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(awsCredentialsProvider)
                .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(region))
                .build()
    }

    /**
     * Initialize an instance of GlueSchemaRegistrySerializer with a configuration object.
     * See documentation for supported configuration entries.
     */
    constructor(
        awsCredentialsProvider: AwsCredentialsProvider,
        glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration,
    ) {
        glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(awsCredentialsProvider)
                .glueSchemaRegistryConfiguration(glueSchemaRegistryConfiguration)
                .build()
    }

    @VisibleForTesting
    internal constructor(glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade) {
        this.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade
    }

    /**
     * Converts the given data byte array to be Glue Schema Registry compatible byte array.
     * If the auto-registration setting is turned on, new schema definitions are automatically
     * registered. The encoded byte array can only be decoded by a Glue Schema Registry
     * de-serializer.
     *
     * @param transportName name of the transport channel for the message, used to add metadata to
     *                      the schema. When null, "default-stream" is used.
     */
    override fun encode(
        transportName: String?,
        schema: Schema,
        data: ByteArray,
    ): ByteArray = glueSchemaRegistrySerializationFacade.encode(transportName, schema, data)
}
