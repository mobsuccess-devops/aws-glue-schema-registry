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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf

import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata.ConnectDataToProtobufDataConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata.ProtobufDataToConnectDataConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema.ProtobufSchemaToConnectSchemaConverter
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.storage.Converter

class ProtobufSchemaConverter(
    private val serializer: GlueSchemaRegistryKafkaSerializer,
    private val deserializer: GlueSchemaRegistryKafkaDeserializer,
) : Converter {
    private var fromConnectSchemaCache: Cache<Schema, Descriptors.FileDescriptor>? = null
    private var toConnectSchemaCache: Cache<Descriptors.Descriptor, Schema>? = null

    private var connectSchemaToProtobufSchemaConverter: ConnectSchemaToProtobufSchemaConverter? = null
    private var connectDataToProtobufDataConverter: ConnectDataToProtobufDataConverter? = null
    private var protobufSchemaToConnectSchemaConverter: ProtobufSchemaToConnectSchemaConverter? = null
    private var protobufDataToConnectDataConverter: ProtobufDataToConnectDataConverter? = null

    private var isKey = false

    constructor() : this(
        GlueSchemaRegistryKafkaSerializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
        GlueSchemaRegistryKafkaDeserializer().apply { userAgentApp = UserAgents.KAFKACONNECT },
    )

    @VisibleForTesting
    internal fun getFromConnectSchemaCache(): Cache<Schema, Descriptors.FileDescriptor>? = fromConnectSchemaCache

    @VisibleForTesting
    internal fun getToConnectSchemaCache(): Cache<Descriptors.Descriptor, Schema>? = toConnectSchemaCache

    override fun config(): ConfigDef = ProtobufSchemaConverterConfig.configDef()

    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        this.isKey = isKey
        ProtobufSchemaConverterConfig(configs)
        val resolvedConfigs = ProtobufSchemaConverterConfig.coerce(configs)

        serializer.configure(resolvedConfigs, this.isKey)
        deserializer.configure(resolvedConfigs, this.isKey)
        connectSchemaToProtobufSchemaConverter = ConnectSchemaToProtobufSchemaConverter()
        connectDataToProtobufDataConverter = ConnectDataToProtobufDataConverter()
        protobufSchemaToConnectSchemaConverter = ProtobufSchemaToConnectSchemaConverter()
        protobufDataToConnectDataConverter = ProtobufDataToConnectDataConverter()

        fromConnectSchemaCache = SynchronizedCache(LRUCache(SCHEMAS_CACHE_SIZE_DEFAULT))
        toConnectSchemaCache = SynchronizedCache(LRUCache(SCHEMAS_CACHE_SIZE_DEFAULT))
    }

    override fun fromConnectData(
        topic: String?,
        schema: Schema,
        value: Any?,
    ): ByteArray {
        val cachedProtobufSchema = fromConnectSchemaCache!!.get(schema)
        if (cachedProtobufSchema != null) {
            val message = connectDataToProtobufDataConverter!!.convert(cachedProtobufSchema, schema, value!!)
            return serializer.serialize(topic, message)!!
        }

        val fileDescriptor = connectSchemaToProtobufSchemaConverter!!.convert(schema)
        fromConnectSchemaCache!!.put(schema, fileDescriptor)
        val message = connectDataToProtobufDataConverter!!.convert(fileDescriptor, schema, value!!)

        return serializer.serialize(topic, message)!!
    }

    override fun toConnectData(
        topic: String?,
        bytes: ByteArray?,
    ): SchemaAndValue {
        val message = deserializer.deserialize(topic, bytes) as Message

        val descriptor = message.descriptorForType
        var schema = toConnectSchemaCache!!.get(descriptor)
        if (schema == null) {
            schema = protobufSchemaToConnectSchemaConverter!!.toConnectSchema(message)
            toConnectSchemaCache!!.put(descriptor, schema)
        }

        return SchemaAndValue(schema, protobufDataToConnectDataConverter!!.toConnectData(message, schema))
    }

    companion object {
        private const val SCHEMAS_CACHE_SIZE_DEFAULT = 50
    }
}
