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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.avro.AvroDeserializer
import com.amazonaws.services.schemaregistry.deserializers.json.JsonDeserializer
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.junit.jupiter.MockitoExtension
import software.amazon.awssdk.services.glue.model.DataFormat

/**
 * Unit tests for testing protocol specific instantiation factory.
 */
@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryDeserializerFactoryTest {
    /**
     * Sets the configuration elements for testing.
     *
     * @return returns a configuration map
     */
    private fun getTestConfigMap(): Map<String, Any> {
        val configMap = HashMap<String, Any>()
        configMap[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()
        configMap[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = AWSSchemaRegistryConstants.COMPRESSION.NONE.name
        configMap[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configMap[AWSSchemaRegistryConstants.AWS_REGION] = "US-West-1"
        return configMap
    }

    /**
     * Test for Avro de-serializer instance creation with combinations of configurations.
     */
    @ParameterizedTest
    @EnumSource(
        value = DataFormat::class,
        mode = EnumSource.Mode.EXCLUDE,
        names = ["UNKNOWN_TO_SDK_VERSION", "JSON", "PROTOBUF"],
    )
    fun testGetInstance_createObject_succeeds(dataFormat: DataFormat) {
        val configMap = getTestConfigMap()

        val configs = GlueSchemaRegistryConfiguration(configMap)
        val glueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()
        val deserializer = glueSchemaRegistryDeserializerFactory.getInstance(dataFormat, configs)

        if (DataFormat.AVRO == dataFormat) {
            assertEquals(AvroDeserializer::class.java, deserializer.javaClass)
        }

        if (DataFormat.JSON == dataFormat) {
            assertEquals(JsonDeserializer::class.java, deserializer.javaClass)
        }
    }

    /**
     * Test for unsupported de-serializer instance creation with combinations of
     * configurations.
     */
    @Test
    fun testGetInstance_UnsupportedDataFormat_throwsException() {
        val glueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()
        assertThrows(UnsupportedOperationException::class.java) {
            glueSchemaRegistryDeserializerFactory.getInstance(
                DataFormat.UNKNOWN_TO_SDK_VERSION,
                GlueSchemaRegistryConfiguration(getTestConfigMap()),
            )
        }
    }

    /**
     * Test for  de-serializer getInstance with null Data Format.
     */
    @Test
    fun testGetInstance_nullDataFormat_throwsException() {
        val glueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryDeserializerFactory.getInstance(
                nullOf(),
                GlueSchemaRegistryConfiguration(getTestConfigMap()),
            )
        }
    }

    /**
     * Test for  de-serializer getInstance with null Data Format.
     */
    @ParameterizedTest
    @EnumSource(value = DataFormat::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNKNOWN_TO_SDK_VERSION"])
    fun testGetInstance_nullConfigs_throwsException(dataFormat: DataFormat) {
        val glueSchemaRegistryDeserializerFactory = GlueSchemaRegistryDeserializerFactory()
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryDeserializerFactory.getInstance(dataFormat, nullOf())
        }
    }
}
