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

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.avro.AvroSerializer
import com.amazonaws.services.schemaregistry.serializers.json.JsonSerializer
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory to create a new instance of protocol specific serializer.
 */
// `open`: the test suites mock this factory.
open class GlueSchemaRegistrySerializerFactory {
    private val serializerMap = ConcurrentHashMap<DataFormat, GlueSchemaRegistryDataFormatSerializer>()

    /**
     * Lazy initializes and returns a specific serializer instance.
     */
    open fun getInstance(
        dataFormat: DataFormat,
        glueSchemaRegistryConfig: GlueSchemaRegistryConfiguration,
    ): GlueSchemaRegistryDataFormatSerializer {
        when (dataFormat) {
            DataFormat.AVRO -> {
                serializerMap.computeIfAbsent(dataFormat) { AvroSerializer() }
                log.debug("Returning Avro serializer instance from GlueSchemaRegistrySerializerFactory")
            }

            DataFormat.JSON -> {
                serializerMap.computeIfAbsent(dataFormat) { JsonSerializer(glueSchemaRegistryConfig) }
                log.debug("Returning Json serializer instance from GlueSchemaRegistrySerializerFactory")
            }

            DataFormat.PROTOBUF -> {
                serializerMap.computeIfAbsent(dataFormat) { ProtobufSerializer(glueSchemaRegistryConfig) }
                log.debug("Returning Protobuf serializer instance from GlueSchemaRegistrySerializerFactory")
            }

            else -> throw AWSSchemaRegistryException("Unsupported data format: $dataFormat")
        }
        return serializerMap[dataFormat]!!
    }

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistrySerializerFactory::class.java)
    }
}
