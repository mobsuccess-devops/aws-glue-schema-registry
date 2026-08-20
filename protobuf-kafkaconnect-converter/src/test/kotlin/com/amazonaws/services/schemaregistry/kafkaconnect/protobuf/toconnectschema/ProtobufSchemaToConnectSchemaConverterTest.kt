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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getAllTypesSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArrayProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getArraySchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getDecimalSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getEnumSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getMapSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getOneofSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getPrimitiveSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getRecursiveProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getStructSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeProtobufMessages
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToConnectTestDataGenerator.getTimeSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.nullOf
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ProtobufSchemaToConnectSchemaConverterTest {
    @BeforeEach
    fun setUp() {
    }

    @ParameterizedTest
    @MethodSource("getPrimitiveTestCases")
    fun toConnectSchema_convertsPrimitiveTypesSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getPrimitiveSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getEnumTestCases")
    fun toConnectSchema_convertsEnumTypesSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getEnumSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getArrayTestCases")
    fun toConnectSchema_convertsArrayTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getArraySchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getMapTestCases")
    fun toConnectSchema_convertsMapTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getMapSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getTimeTestCases")
    fun toConnectSchema_convertsTimeTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getTimeSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getDecimalTestCases")
    fun toConnectSchema_convertsDecimalTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getDecimalSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getStructTestCases")
    fun toConnectSchema_convertsStructTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getStructSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getOneofTestCases")
    fun toConnectSchema_convertsOneofTypeSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getOneofSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getAllTypesTestCases")
    fun toConnectSchema_convertsAllTypesSchema(message: Message) {
        val packageName = message.descriptorForType.file.`package`
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        val expectedConnectSchema = getAllTypesSchema(packageName)
        assertEquals(expectedConnectSchema, actualConnectSchema)
    }

    @ParameterizedTest
    @MethodSource("getRecursiveTestCases")
    fun toConnectSchema_convertsRecursiveSchema(message: Message) {
        val actualConnectSchema = PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(message)
        assertEquals(Schema.Type.STRUCT, actualConnectSchema.type())

        val messageName = message.descriptorForType.name
        assertEquals(messageName, actualConnectSchema.name())

        if ("RecursiveType" == messageName) {
            assertEquals(3, actualConnectSchema.fields().size)
            assertEquals("name", actualConnectSchema.field("name").name())
            assertEquals("parent", actualConnectSchema.field("parent").name())
            assertEquals("children", actualConnectSchema.field("children").name())
        } else if ("TreeNode" == messageName) {
            assertEquals(3, actualConnectSchema.fields().size)
            assertEquals("value", actualConnectSchema.field("value").name())
            assertEquals("left", actualConnectSchema.field("left").name())
            assertEquals("right", actualConnectSchema.field("right").name())
        }
    }

    @Test
    fun toConnectSchema_forNullMessage_ThrowsException() {
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER.toConnectSchema(nullOf())
        }
    }

    companion object {
        private val PROTOBUF_SCHEMA_TO_CONNECT_SCHEMA_CONVERTER = ProtobufSchemaToConnectSchemaConverter()

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
        fun getAllTypesTestCases(): Stream<Arguments> = getAllTypesProtobufMessages().stream().map { Arguments.of(it) }

        @JvmStatic
        fun getRecursiveTestCases(): Stream<Arguments> = getRecursiveProtobufMessages().stream().map { Arguments.of(it) }
    }
}
