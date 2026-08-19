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

package com.amazonaws.services.schemaregistry.flink.avro

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import org.apache.avro.Schema
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.io.IOException
import java.io.OutputStream

/**
 * AWS Glue Schema Registry output stream serializer to accept schema and output stream to register
 * schema and write the serialized object with schema registry bytes to the output stream.
 */
// `open`: the test suites mock this type.
open class GlueSchemaRegistryOutputStreamSerializer(
    private val transportName: String?,
    private val configs: Map<String, Any>,
    glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade?,
) {
    constructor(transportName: String?, configs: Map<String, Any>) : this(transportName, configs, null)

    private val glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade =
        glueSchemaRegistrySerializationFacade
            ?: GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(DefaultCredentialsProvider.builder().build())
                .glueSchemaRegistryConfiguration(GlueSchemaRegistryConfiguration(configs))
                .build()

    init {
        GlueSchemaRegistryConfiguration(configs).userAgentApp = UserAgents.FLINK
    }

    /**
     * Register the schema and write the serialized object with schema registry bytes to the stream.
     */
    @Throws(IOException::class)
    open fun registerSchemaAndSerializeStream(
        schema: Schema,
        out: OutputStream,
        data: ByteArray,
    ) {
        val bytes =
            glueSchemaRegistrySerializationFacade.encode(
                transportName,
                com.amazonaws.services.schemaregistry.common.Schema(
                    schema.toString(),
                    DataFormat.AVRO.toString(),
                    getSchemaName()!!,
                ),
                data,
            )
        out.write(bytes)
    }

    private fun getSchemaName(): String? =
        GlueSchemaRegistryUtils.getInstance().getSchemaName(configs)
            ?: GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs)!!.getSchemaName(transportName)
}
