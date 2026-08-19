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

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.avro.AvroDeserializer
import com.amazonaws.services.schemaregistry.deserializers.json.JsonDeserializer
import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufDeserializer
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory to create a new instance of protocol specific de-serializer.
 */
// `open`: the test suites mock this factory.
open class GlueSchemaRegistryDeserializerFactory {
    private val deserializerMap = ConcurrentHashMap<DataFormat, GlueSchemaRegistryDataFormatDeserializer>()

    /**
     * Lazy initializes and returns a specific de-serializer instance.
     */
    open fun getInstance(
        dataFormat: DataFormat,
        configs: GlueSchemaRegistryConfiguration,
    ): GlueSchemaRegistryDataFormatDeserializer {
        when (dataFormat) {
            DataFormat.AVRO -> {
                deserializerMap.computeIfAbsent(dataFormat) { AvroDeserializer.builder().configs(configs).build() }
                log.debug("Returning Avro de-serializer instance from GlueSchemaRegistryDeserializerFactory")
            }

            DataFormat.JSON -> {
                deserializerMap.computeIfAbsent(dataFormat) { JsonDeserializer.builder().configs(configs).build() }
                log.debug("Returning JSON de-serializer instance from GlueSchemaRegistryDeserializerFactory")
            }

            DataFormat.PROTOBUF -> {
                deserializerMap.computeIfAbsent(dataFormat) { ProtobufDeserializer.builder().configs(configs).build() }
                log.debug("Returning Protobuf de-serializer instance from GlueSchemaRegistryDeserializerFactory")
            }

            else -> throw UnsupportedOperationException("Data Format is not supported $dataFormat")
        }
        return deserializerMap[dataFormat]!!
    }

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryDeserializerFactory::class.java)
    }
}
