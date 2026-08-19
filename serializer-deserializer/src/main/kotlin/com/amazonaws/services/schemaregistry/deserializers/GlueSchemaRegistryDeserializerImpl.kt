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

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.google.common.annotations.VisibleForTesting
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * {@inheritDoc}
 */
class GlueSchemaRegistryDeserializerImpl : GlueSchemaRegistryDeserializer {
    private val glueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade

    /**
     * Initialize an instance of GlueSchemaRegistryDeserializer with Properties.
     * See documentation for supported configuration property format.
     */
    constructor(
        awsCredentialsProvider: AwsCredentialsProvider,
        configuration: GlueSchemaRegistryConfiguration,
    ) {
        glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade(configuration, awsCredentialsProvider)
    }

    @VisibleForTesting
    protected constructor(glueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade) {
        this.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade
    }

    override fun overrideUserAgentApp(name: String?) {
        glueSchemaRegistryDeserializationFacade.overrideUserAgentApp(name)
    }

    override fun getData(data: ByteArray): ByteArray = glueSchemaRegistryDeserializationFacade.getActualData(data)

    override fun getSchema(data: ByteArray): Schema = glueSchemaRegistryDeserializationFacade.getSchema(data)

    override fun canDeserialize(data: ByteArray?): Boolean = glueSchemaRegistryDeserializationFacade.canDeserialize(data)
}
