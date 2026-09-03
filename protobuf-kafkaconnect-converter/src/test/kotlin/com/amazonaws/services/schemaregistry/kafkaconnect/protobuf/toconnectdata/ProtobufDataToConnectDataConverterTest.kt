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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArraySchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getNestedOneofProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getNestedOneofSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getNestedOneofTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveTypesData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeTypeData
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.nullOf
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema.ProtobufSchemaToConnectSchemaConverter
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ProtobufDataToConnectDataConverterTest {
    @ParameterizedTest
    @MethodSource("getPrimitiveTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forPrimitiveTypes(primitiveMessage: Message) {
        val packageName = primitiveMessage.descriptorForType.file.`package`
        val connectSchema = getPrimitiveSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(primitiveMessage, connectSchema)
        val expectedData = getPrimitiveTypesData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getEnumTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forEnumType(enumMessage: Message) {
        val packageName = enumMessage.descriptorForType.file.`package`
        val connectSchema = getEnumSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(enumMessage, connectSchema)
        val expectedData = getEnumTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getArrayTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forArrayType(arrayMessage: Message) {
        val packageName = arrayMessage.descriptorForType.file.`package`
        val connectSchema = getArraySchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(arrayMessage, connectSchema)
        val expectedData = getArrayTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getMapTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forMapType(mapMessage: Message) {
        val packageName = mapMessage.descriptorForType.file.`package`
        val connectSchema = getMapSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(mapMessage, connectSchema)
        val expectedData = getMapTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getTimeTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forTimeType(timeMessage: Message) {
        val packageName = timeMessage.descriptorForType.file.`package`
        val connectSchema = getTimeSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(timeMessage, connectSchema)
        val expectedData = getTimeTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getDecimalTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forDecimalType(decimalMessage: Message) {
        val packageName = decimalMessage.descriptorForType.file.`package`
        val connectSchema = getDecimalSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(decimalMessage, connectSchema)
        val expectedData = getDecimalTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getStructTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forStructType(nestedMessage: Message) {
        val packageName = nestedMessage.descriptorForType.file.`package`
        val connectSchema = getStructSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(nestedMessage, connectSchema)
        val expectedData = getStructTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getOneofTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forOneofType(oneofMessage: Message) {
        val packageName = oneofMessage.descriptorForType.file.`package`
        val connectSchema = getOneofSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(oneofMessage, connectSchema)
        val expectedData = getOneofTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getAllTypesTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forAllTypes(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val connectSchema = getAllTypesSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, connectSchema)
        val expectedData = getAllTypesData(packageName)

        assertEquals(expectedData, actualData)
    }

    @Test
    fun toConnectData_ForInvalidClassCasts_ThrowsDataException() {
        val message = getPrimitiveProtobufMessages()[0]
        val nonMatchingSchema =
            SchemaBuilder(Schema.Type.STRUCT).field("i8", SchemaBuilder.string()).build()
        val ex =
            assertThrows(DataException::class.java) {
                PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, nonMatchingSchema)
            }
        assertEquals(
            "Error converting value: \"2\" (Java Type: class java.lang.Integer, Protobuf type: INT32) " +
                "to Connect type: STRING",
            ex.message,
        )
    }

    @Test
    fun toConnectData_ForMissingField_ThrowsDataException() {
        val message = getPrimitiveProtobufMessages()[0]
        val nonMatchingSchema =
            SchemaBuilder(Schema.Type.STRUCT).field("invalidSchema", SchemaBuilder.string()).build()
        val ex =
            assertThrows(DataException::class.java) {
                PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(message, nonMatchingSchema)
            }
        assertEquals(
            "Protobuf schema doesn't contain the connect field: invalidSchema",
            ex.message,
        )
    }

    @Test
    fun toConnectData_ForNullParams_ThrowsException() {
        val anyMessage = getPrimitiveProtobufMessages()[0]
        val anySchema = ToConnectTestDataGenerator.getPrimitiveSchema("any")
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(nullOf(), anySchema)
        }
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(anyMessage, nullOf())
        }
    }

    @ParameterizedTest
    @MethodSource("getNestedOneofTestCases")
    fun toConnectData_convertsProtobufMessageToConnect_forNestedOneofType(nestedOneofMessage: Message) {
        val packageName = nestedOneofMessage.descriptorForType.file.`package`
        val connectSchema = getNestedOneofSchema(packageName)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(nestedOneofMessage, connectSchema)
        val expectedData = getNestedOneofTypeData(packageName)

        assertEquals(expectedData, actualData)
    }

    @ParameterizedTest
    @MethodSource("getNestedOneofTestCases")
    fun toConnectData_underTheConvertedSchema_validatesForNestedOneofType(nestedOneofMessage: Message) {
        val connectSchema = ProtobufSchemaToConnectSchemaConverter().toConnectSchema(nestedOneofMessage)
        val actualData = PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER.toConnectData(nestedOneofMessage, connectSchema)

        assertDoesNotThrow { (actualData as Struct).validate() }
    }

    companion object {
        private val PROTOBUF_DATA_TO_CONNECT_DATA_CONVERTER = ProtobufDataToConnectDataConverter()

        @JvmStatic
        fun getPrimitiveTestCases(): Stream<Arguments> = getPrimitiveProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getEnumTestCases(): Stream<Arguments> = getEnumProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getArrayTestCases(): Stream<Arguments> = getArrayProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getMapTestCases(): Stream<Arguments> = getMapProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getTimeTestCases(): Stream<Arguments> = getTimeProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getDecimalTestCases(): Stream<Arguments> = getDecimalProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getStructTestCases(): Stream<Arguments> = getStructProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getOneofTestCases(): Stream<Arguments> = getOneofProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getNestedOneofTestCases(): Stream<Arguments> = getNestedOneofProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getAllTypesTestCases(): Stream<Arguments> = getAllTypesProtobufMessages().stream().map { Arguments.of(it) }
    }
}
