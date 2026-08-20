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

package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufWireFormatDecoder
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.internal.parser.ProtoParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.Optional
import java.util.stream.Stream

class ProtobufSerializerTest {
    private val protobufSerializer =
        ProtobufSerializer(
            GlueSchemaRegistryConfiguration(hashMapOf(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2")),
        )
    private val protobufWireFormatDecoder = ProtobufWireFormatDecoder(MessageIndexFinder())

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    fun testSerialize_ProducesValidDeserializableBytes_ForAllTypesOfMessages(message: Message) {
        val serializedBytes = protobufSerializer.serialize(message)

        val deserializedDynamicMessage =
            protobufWireFormatDecoder.decode(
                serializedBytes,
                getFileDescriptor(message),
                ProtobufMessageType.DYNAMIC_MESSAGE,
            ) as DynamicMessage
        val deserializedPojoMessage =
            protobufWireFormatDecoder.decode(
                serializedBytes,
                getFileDescriptor(message),
                ProtobufMessageType.POJO,
            ) as Message

        // Assert that the message can de-serialized back into original Message.
        assertEquals(message, deserializedDynamicMessage)
        assertEquals(message, deserializedPojoMessage)
    }

    @ParameterizedTest
    @MethodSource("testProtobufSchemaDefinitionProvider")
    fun testGetSchemaDefinition_GeneratesValidSchemaDefinition_ForAllTypesOfMessages(
        message: Message,
        schemaDefinition: String,
        schemaName: String,
    ) {
        val parsedSchemaDefinition = protobufSerializer.getSchemaDefinition(message)
        assertFalse(parsedSchemaDefinition.contains("// Proto schema formatted by Wire, do not edit.\n// Source: \n\n"))
        val packageName = ProtoParser.parse(Location.get(""), schemaDefinition).packageName

        val expectedFileDescriptorProto =
            FileDescriptorUtils
                .protoFileToFileDescriptor(schemaDefinition, schemaName, Optional.ofNullable(packageName))
                .toProto()

        val parsedFileDescriptorProto =
            FileDescriptorUtils
                .protoFileToFileDescriptor(parsedSchemaDefinition, schemaName, Optional.ofNullable(packageName))
                .toProto()
        assertEquals(expectedFileDescriptorProto, parsedFileDescriptorProto)
    }

    @Test
    fun testValidate_invalidObject_throwsException() {
        val s = "test"
        var ex: Exception = assertThrows(AWSSchemaRegistryException::class.java) { protobufSerializer.validate(s) }
        assertEquals("Object is not of Message type: class java.lang.String", ex.message)

        val num = 5
        ex = assertThrows(AWSSchemaRegistryException::class.java) { protobufSerializer.validate(num) }
        assertEquals("Object is not of Message type: class java.lang.Integer", ex.message)

        ex = assertThrows(NullPointerException::class.java) { protobufSerializer.validate(nullOf()) }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services.schemaregistry." +
                "serializers.protobuf.ProtobufSerializer.validate, parameter data",
            ex.message,
        )
    }

    @Test
    fun testSerialize_CachesGeneratedSchema_ForSameArguments() {
        // Get schema definition for repeated messages of different types.
        testMessageProviderForCaching()
            .map { it.get() }
            .map { objects -> objects[0] }
            .forEach { protobufSerializer.getSchemaDefinition(it) }

        assertEquals(3, protobufSerializer.schemaGeneratorCache.size())
    }

    private fun getFileDescriptor(message: Message): Descriptors.FileDescriptor = message.descriptorForType.file

    companion object {
        @JvmStatic
        fun testMessageProvider(): Stream<Arguments> = Stream.of(
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX2_MESSAGE),
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX3_MESSAGE),
            Arguments.of(ProtobufGenerator.BASIC_REFERENCING_MESSAGE),
            Arguments.of(ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE),
            Arguments.of(ProtobufGenerator.JAVA_OUTER_CLASS_MESSAGE),
            Arguments.of(ProtobufGenerator.JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE),
            Arguments.of(ProtobufGenerator.NESTING_MESSAGE_PROTO3),
            Arguments.of(ProtobufGenerator.NESTING_MESSAGE_PROTO2),
            Arguments.of(ProtobufGenerator.SNAKE_CASE_MESSAGE),
            Arguments.of(ProtobufGenerator.ANOTHER_SNAKE_CASE_MESSAGE),
            Arguments.of(ProtobufGenerator.DOLLAR_SYNTAX_3_MESSAGE),
            Arguments.of(ProtobufGenerator.HYPHEN_ATED_PROTO_FILE_MESSAGE),
            Arguments.of(ProtobufGenerator.DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE),
            Arguments.of(ProtobufGenerator.UNICODE_MESSAGE),
            Arguments.of(ProtobufGenerator.CONFLICTING_NAME_MESSAGE),
            Arguments.of(ProtobufGenerator.NESTED_CONFLICTING_NAME_MESSAGE),
            Arguments.of(ProtobufGenerator.NESTING_MESSAGE_PROTO3_MULTIPLE_FILES),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(ProtobufGenerator.createDynamicProtobufRecord()),
            Arguments.of(ProtobufGenerator.createCompiledProtobufRecord()),
            Arguments.of(ProtobufGenerator.createCompiledProtobufRecord()),
        )

        @JvmStatic
        fun testMessageProviderForCaching(): Stream<Arguments> = Stream.of(
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX2_MESSAGE),
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX3_MESSAGE),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(ProtobufGenerator.createDynamicNRecord()),
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX3_MESSAGE),
        )

        @JvmStatic
        fun testProtobufSchemaDefinitionProvider(): Stream<Arguments> = Stream.of(
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO2,
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax2.proto").getRawSchema(),
                "ComplexNestingSyntax2.proto",
            ),
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO3,
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax3.proto").getRawSchema(),
                "ComplexNestingSyntax3.proto",
            ),
            Arguments.of(
                ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("Basic.proto").getRawSchema(),
                "Basic.proto",
            ),
            Arguments.of(
                ProtobufGenerator.BASIC_SYNTAX3_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("basicsyntax3.proto").getRawSchema(),
                "basicsyntax3",
            ),
            Arguments.of(
                ProtobufGenerator.BASIC_SYNTAX2_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("basicSyntax2.proto").getRawSchema(),
                "basicSyntax2",
            ),
            Arguments.of(
                ProtobufGenerator.DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE,
                ProtobufTestCaseReader
                    .getTestCaseByName(".protodevelasl.proto.proto.protodevel\$---\$\$.bar.3.proto")
                    .getRawSchema(),
                ".protodevelasl.proto.proto.protodevel\$---\$\$.bar.3",
            ),
            Arguments.of(
                ProtobufGenerator.UNICODE_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("◉◉◉unicode⏩.proto").getRawSchema(),
                "◉◉◉unicode⏩.proto",
            ),
            Arguments.of(
                ProtobufGenerator.DOLLAR_SYNTAX_3_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("foo\$\$\$1.proto").getRawSchema(),
                "foo\$\$\$1.proto",
            ),
            Arguments.of(
                ProtobufGenerator.createDynamicProtobufRecord(),
                ProtobufTestCaseReader.getTestCaseByName("Basic.proto").getRawSchema(),
                "Basic.proto",
            ),
        )
    }
}
