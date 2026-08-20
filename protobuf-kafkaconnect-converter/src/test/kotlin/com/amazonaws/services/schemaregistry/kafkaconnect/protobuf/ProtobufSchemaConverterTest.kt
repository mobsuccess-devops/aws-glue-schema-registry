/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.common.configs.UserAgents
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer
import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufWireFormatDecoder
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doNothing
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.glue.model.DataFormat
import java.util.stream.Stream

class ProtobufSchemaConverterTest {
    private lateinit var protobufSchemaConverter: ProtobufSchemaConverter

    @Mock
    private lateinit var serializer: GlueSchemaRegistryKafkaSerializer

    @Mock
    private lateinit var deserializer: GlueSchemaRegistryKafkaDeserializer

    @BeforeEach
    fun setUp() {
        val config = getSchemaRegistryConfig()
        MockitoAnnotations.openMocks(this)
        protobufSchemaConverter = ProtobufSchemaConverter(serializer, deserializer)
        protobufSchemaConverter.configure(config, false)

        doNothing().whenever(serializer).configure(config, false)
        doNothing().whenever(serializer).userAgentApp = UserAgents.KAFKACONNECT

        doNothing().whenever(deserializer).configure(config, false)
        doNothing().whenever(deserializer).userAgentApp = UserAgents.KAFKACONNECT
    }

    private fun getSchemaRegistryConfig(): Map<String, *> = mapOf(
        AWSSchemaRegistryConstants.AWS_REGION to "us-east-1",
        AWSSchemaRegistryConstants.DATA_FORMAT to DataFormat.PROTOBUF.name,
    )

    @Test
    fun initializesConverter_Successfully() {
        assertDoesNotThrow { ProtobufSchemaConverter() }
    }

    @ParameterizedTest
    @MethodSource("getFromConnectTestCases")
    fun fromConnectData_convertsConnectDataToGSRSerializedProtobufData(
        connectData: Any,
        connectSchema: Schema,
        protobufData: DynamicMessage,
    ) {
        val argumentCaptor = argumentCaptor<DynamicMessage>()
        doReturn(ByteArray(0)).whenever(serializer).serialize(eq(TOPIC_NAME), any())
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectSchema, connectData)
        verify(serializer, times(1)).serialize(eq(TOPIC_NAME), argumentCaptor.capture())

        assertEquals(protobufData.toString(), argumentCaptor.lastValue.toString())
    }

    @ParameterizedTest
    @MethodSource("getToConnectTestCases")
    fun toConnectData_convertsProtobufSerializedDataToConnectData(
        protobufData: Message,
        connectSchema: Schema,
        connectData: Any,
    ) {
        val serializedData = protobufData.toByteArray()

        doReturn(protobufData).whenever(deserializer).deserialize(TOPIC_NAME, serializedData)

        val schemaAndValue = protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedData)

        val expectedSchemaAndValue = SchemaAndValue(connectSchema, connectData)
        assertEquals(expectedSchemaAndValue, schemaAndValue)
    }

    @Test
    fun endToEndTest_forAllTypesSchema() {
        val connectData: Any = ToProtobufTestDataGenerator.getAllTypesData("AllTypes")
        val connectSchema = ToProtobufTestDataGenerator.getAllTypesSchema("AllTypes")
        val protobufData = ToProtobufTestDataGenerator.getProtobufAllTypesMessage("AllTypes")

        // from connect
        val argumentCaptor = argumentCaptor<Message>()
        doReturn(ByteArray(0)).whenever(serializer).serialize(eq(TOPIC_NAME), any())
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectSchema, connectData)
        verify(serializer, times(1)).serialize(eq(TOPIC_NAME), argumentCaptor.capture())

        // assert that ConnectToProtobuf conversion is correct
        assertEquals(protobufData.toString(), argumentCaptor.lastValue.toString())

        val protobufSerializer =
            ProtobufSerializer(
                GlueSchemaRegistryConfiguration(
                    hashMapOf(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2"),
                ),
            )

        // serializedData is the raw data from the serialization
        val serializedData = protobufSerializer.serialize(protobufData)

        // fileDescriptor is the schema definition reconstructed from connect data and is registered with Glue Schema
        // Registry service
        val fileDescriptor = ConnectSchemaToProtobufSchemaConverter().convert(connectSchema)
        val protobufWireFormatDecoder = ProtobufWireFormatDecoder(MessageIndexFinder())
        val deserializedMessage =
            protobufWireFormatDecoder.decode(
                serializedData,
                fileDescriptor,
                ProtobufMessageType.DYNAMIC_MESSAGE,
            ) as DynamicMessage
        // Mockito is used to mock the actual deserialization result
        doReturn(deserializedMessage).whenever(deserializer).deserialize(TOPIC_NAME, serializedData)

        // to connect
        val schemaAndValue = protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedData)

        // assert that end to end conversion result is the same as the input
        val actualSchema = schemaAndValue.schema()
        val actualData = schemaAndValue.value()
        assertEquals(connectSchema.name(), actualSchema.name())
        assertEquals(connectSchema.fields().size, actualSchema.fields().size)
        for (field in connectSchema.fields()) {
            val fieldName = field.name()
            val actualField = actualSchema.field(fieldName)
            // assert each field schema is the same
            assertEquals(field, actualField, fieldName)

            val expectedFieldObject = (connectData as Struct).get(field)
            val actualFieldObject = (actualData as Struct).get(actualField)
            // assert each field value is the same
            if (field.name() == "bytes") {
                assertTrue((expectedFieldObject as ByteArray).contentEquals(actualFieldObject as ByteArray))
            } else {
                assertEquals(expectedFieldObject, actualFieldObject, fieldName)
            }
        }
    }

    @Test
    fun testSchemaCache_toConnectConversion() {
        val toConnectSchemaCache = protobufSchemaConverter.getToConnectSchemaCache()!!
        assertEquals(0, toConnectSchemaCache.size())

        val protobufPrimitiveData = ToConnectTestDataGenerator.getPrimitiveProtobufMessages()[0]
        val serializedPrimitiveData = protobufPrimitiveData.toByteArray()
        doReturn(protobufPrimitiveData).whenever(deserializer).deserialize(TOPIC_NAME, serializedPrimitiveData)
        protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedPrimitiveData)
        assertEquals(1, toConnectSchemaCache.size())

        // converting the same schema to see if the cache is working properly
        doReturn(protobufPrimitiveData).whenever(deserializer).deserialize(TOPIC_NAME, serializedPrimitiveData)
        protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedPrimitiveData)
        assertEquals(1, toConnectSchemaCache.size())

        val protobufEnumData = ToConnectTestDataGenerator.getEnumProtobufMessages()[0]
        val serializedEnumData = protobufEnumData.toByteArray()
        doReturn(protobufEnumData).whenever(deserializer).deserialize(TOPIC_NAME, serializedEnumData)
        protobufSchemaConverter.toConnectData(TOPIC_NAME, serializedEnumData)
        assertEquals(2, toConnectSchemaCache.size())
    }

    @Test
    fun testSchemaCache_fromConnectConversion() {
        val fromConnectSchemaCache = protobufSchemaConverter.getFromConnectSchemaCache()!!
        assertEquals(0, fromConnectSchemaCache.size())

        doReturn(ByteArray(0)).whenever(serializer).serialize(eq(TOPIC_NAME), any())
        val connectPrimitiveData: Any = ToProtobufTestDataGenerator.getPrimitiveTypesData()
        val connectPrimitiveSchema = ToProtobufTestDataGenerator.getPrimitiveSchema(SCHEMA_NAME)
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectPrimitiveSchema, connectPrimitiveData)
        assertEquals(1, fromConnectSchemaCache.size())

        // converting the same schema to see if the cache is working properly
        doReturn(ByteArray(0)).whenever(serializer).serialize(eq(TOPIC_NAME), any())
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectPrimitiveSchema, connectPrimitiveData)
        assertEquals(1, fromConnectSchemaCache.size())

        doReturn(ByteArray(0)).whenever(serializer).serialize(eq(TOPIC_NAME), any())
        val connectEnumData: Any = ToProtobufTestDataGenerator.getEnumTypeData("EnumType")
        val connectEnumSchema = ToProtobufTestDataGenerator.getEnumSchema("EnumType")
        protobufSchemaConverter.fromConnectData(TOPIC_NAME, connectEnumSchema, connectEnumData)
        assertEquals(2, fromConnectSchemaCache.size())
    }

    companion object {
        private const val TOPIC_NAME = "Foo"
        private const val SCHEMA_NAME = "ProtobufConverterTest"
        private const val PACKAGE_NAME = "com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3"

        @JvmStatic
        fun getFromConnectTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(
                ToProtobufTestDataGenerator.getPrimitiveTypesData(),
                ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufPrimitiveMessage(),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getEnumTypeData("EnumType"),
                ToProtobufTestDataGenerator.getEnumSchema("EnumType"),
                ToProtobufTestDataGenerator.getProtobufEnumMessage("EnumType"),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getArrayTypeData(),
                ToProtobufTestDataGenerator.getArraySchema("arrayProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufArrayMessage(),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getMapTypeData(),
                ToProtobufTestDataGenerator.getMapSchema("mapProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufMapMessage(),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getTimeTypeData(),
                ToProtobufTestDataGenerator.getTimeSchema("timeProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufTimeMessage(),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getStructTypeData("NestedType"),
                ToProtobufTestDataGenerator.getStructSchema("NestedType"),
                ToProtobufTestDataGenerator.getProtobufNestedMessage("NestedType"),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getOneofTypeData(),
                ToProtobufTestDataGenerator.getOneofSchema("oneofProtobufSchema"),
                ToProtobufTestDataGenerator.getProtobufOneofMessage(),
            ),
            Arguments.of(
                ToProtobufTestDataGenerator.getAllTypesData("AllTypes"),
                ToProtobufTestDataGenerator.getAllTypesSchema("AllTypes"),
                ToProtobufTestDataGenerator.getProtobufAllTypesMessage("AllTypes"),
            ),
        )

        @JvmStatic
        fun getToConnectTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(
                ToConnectTestDataGenerator.getPrimitiveProtobufMessages()[0],
                ToConnectTestDataGenerator.getPrimitiveSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getPrimitiveTypesData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getEnumProtobufMessages()[0],
                ToConnectTestDataGenerator.getEnumSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getEnumTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getArrayProtobufMessages()[0],
                ToConnectTestDataGenerator.getArraySchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getArrayTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getMapProtobufMessages()[0],
                ToConnectTestDataGenerator.getMapSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getMapTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getTimeProtobufMessages()[0],
                ToConnectTestDataGenerator.getTimeSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getTimeTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getStructProtobufMessages()[0],
                ToConnectTestDataGenerator.getStructSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getStructTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getOneofProtobufMessages()[0],
                ToConnectTestDataGenerator.getOneofSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getOneofTypeData(PACKAGE_NAME),
            ),
            Arguments.of(
                ToConnectTestDataGenerator.getAllTypesProtobufMessages()[0],
                ToConnectTestDataGenerator.getAllTypesSchema(PACKAGE_NAME),
                ToConnectTestDataGenerator.getAllTypesData(PACKAGE_NAME),
            ),
        )
    }
}
