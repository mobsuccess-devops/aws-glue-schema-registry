package com.amazonaws.services.schemaregistry.deserializers.protobuf

import Foo.Contact
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.BasicSyntax2
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.basic.ProtodevelaslProtoProtoProtodevelBar3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.snake_case.SnakeCaseFile
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Basicsyntax3
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.ConflictingNameOuterClass
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Foo1
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.HyphenAtedProtoFile
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.NestedConflictingClassNameOuterClass
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.basic.Unicode
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.snake_case.AnotherSnakeCaseProtoFile
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.protobuf.DynamicMessage
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

class ProtobufWireFormatDecoderTest {
    private val configs = GlueSchemaRegistryConfiguration("us-west-2")
    private val protobufSerializer = ProtobufSerializer(configs)
    private val decoder = ProtobufWireFormatDecoder(MessageIndexFinder())
    private val basicTestCase = ProtobufTestCaseReader.getTestCaseByName("Basic.proto")
    private val basicFileDescriptor = basicTestCase.getSchema()

    @Test
    fun testDecodeDynamicMessage_NullInputStream_ThrowsException() {
        val ex =
            assertThrows(NullPointerException::class.java) {
                decoder.decode(nullOf(), basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE)
            }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services.schemaregistry" +
                ".deserializers.protobuf.ProtobufWireFormatDecoder.decode, parameter data",
            ex.message,
        )
    }

    @Test
    fun testDecodeDynamicMessage_NullDescriptor_ThrowsException() {
        val ex =
            assertThrows(NullPointerException::class.java) {
                decoder.decode(ByteArray(0), nullOf(), ProtobufMessageType.DYNAMIC_MESSAGE)
            }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services.schemaregistry" +
                ".deserializers.protobuf.ProtobufWireFormatDecoder.decode, parameter descriptor",
            ex.message,
        )
    }

    @ParameterizedTest
    @MethodSource("getDynamicMessageDecoderTestCases")
    fun testDecode_UnknownMessageTypeValidInputs_ToDynamicMessage_Succeeds(
        dynamicMessage: DynamicMessage,
        protobufMessageType: ProtobufMessageType?,
    ) {
        val serializedBytes = protobufSerializer.serialize(dynamicMessage)
        val decoded =
            decoder.decode(
                serializedBytes,
                dynamicMessage.descriptorForType.file,
                protobufMessageType,
            ) as DynamicMessage
        assertArrayEquals(dynamicMessage.toByteArray(), decoded.toByteArray())
    }

    @Test
    fun testDecode_DynamicMessage_CorruptedMessageIndex_ThrowsException() {
        val invalidData = "😋".toByteArray(StandardCharsets.UTF_8)
        val ex =
            assertThrows(InvalidProtocolBufferException::class.java) {
                decoder.decode(invalidData, basicFileDescriptor, ProtobufMessageType.DYNAMIC_MESSAGE)
            }
        assertEquals(
            "While parsing a protocol message, " +
                "the input ended unexpectedly in the middle of a field.  This could mean either that the input has " +
                "been truncated or that an embedded message misreported its own length.",
            ex.message,
        )
    }

    @ParameterizedTest
    @MethodSource("getPOJODecoderTestCases")
    fun testDecode_WhenMessagesArePassed_DeserializesThemIntoCorrectPOJOs(
        message: Message,
        expectedClass: Class<*>,
    ) {
        val data = protobufSerializer.serialize(message)
        val fileDescriptor = message.descriptorForType.file

        val decodedObject = decoder.decode(data, fileDescriptor, ProtobufMessageType.POJO)

        assertTrue(expectedClass.isInstance(decodedObject))
        assertEquals(message, decodedObject)
    }

    @Test
    fun testDecode_WhenPOJOClassIsNotFound_ThrowsRuntimeException() {
        val nonPOJOExistentMessage = ProtobufGenerator.createRuntimeCompiledRecord()
        val nonExistentMessageBytes = protobufSerializer.serialize(nonPOJOExistentMessage)

        val ex =
            assertThrows(RuntimeException::class.java) {
                decoder.decode(
                    nonExistentMessageBytes,
                    nonPOJOExistentMessage.descriptorForType.file,
                    ProtobufMessageType.POJO,
                )
            }
        assertEquals("Error de-serializing data into Message class: foo.NonExistent\$NonExistentSchema", ex.message)

        val rootCause = ex.cause!!
        assertEquals(ClassNotFoundException::class.java, rootCause.javaClass)
        assertEquals("foo.NonExistent\$NonExistentSchema", rootCause.message)
    }

    companion object {
        @JvmStatic
        fun getPOJODecoderTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX2_MESSAGE, BasicSyntax2.Phone::class.java),
            Arguments.of(ProtobufGenerator.BASIC_SYNTAX3_MESSAGE, Basicsyntax3.Phone::class.java),
            Arguments.of(ProtobufGenerator.BASIC_REFERENCING_MESSAGE, Basic.Customer::class.java),
            Arguments.of(ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE, Basic.Address::class.java),
            Arguments.of(ProtobufGenerator.JAVA_OUTER_CLASS_MESSAGE, Contact.Phone::class.java),
            Arguments.of(
                ProtobufGenerator.JAVA_OUTER_CLASS_WITH_MULTIPLE_FILES_MESSAGE,
                com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.Phone::class.java,
            ),
            Arguments.of(ProtobufGenerator.NESTING_MESSAGE_PROTO3, ComplexNestingSyntax3.A.B.C.X.D.F.M::class.java),
            Arguments.of(ProtobufGenerator.NESTING_MESSAGE_PROTO2, ComplexNestingSyntax2.O.A::class.java),
            Arguments.of(ProtobufGenerator.SNAKE_CASE_MESSAGE, SnakeCaseFile.snake_case_message::class.java),
            Arguments.of(
                ProtobufGenerator.ANOTHER_SNAKE_CASE_MESSAGE,
                AnotherSnakeCaseProtoFile.another_SnakeCase_::class.java,
            ),
            Arguments.of(ProtobufGenerator.DOLLAR_SYNTAX_3_MESSAGE, Foo1.Dollar::class.java),
            Arguments.of(
                ProtobufGenerator.HYPHEN_ATED_PROTO_FILE_MESSAGE,
                HyphenAtedProtoFile.hyphenated::class.java,
            ),
            Arguments.of(
                ProtobufGenerator.DOUBLE_PROTO_WITH_TRAILING_HASH_MESSAGE,
                ProtodevelaslProtoProtoProtodevelBar3.bar::class.java,
            ),
            Arguments.of(ProtobufGenerator.UNICODE_MESSAGE, Unicode.uni::class.java),
            Arguments.of(
                ProtobufGenerator.CONFLICTING_NAME_MESSAGE,
                ConflictingNameOuterClass.ConflictingName::class.java,
            ),
            Arguments.of(
                ProtobufGenerator.NESTED_CONFLICTING_NAME_MESSAGE,
                NestedConflictingClassNameOuterClass.Parent.NestedConflictingClassName::class.java,
            ),
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO3_MULTIPLE_FILES,
                com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.multiplefiles.A.B.C.X.D.F.M::class.java,
            ),
        )

        @JvmStatic
        fun getDynamicMessageDecoderTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(ProtobufGenerator.BASIC_REFERENCING_DYNAMIC_MESSAGE, ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(ProtobufGenerator.createDynamicNRecord(), ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(ProtobufGenerator.createDynamicProtobufRecord(), ProtobufMessageType.DYNAMIC_MESSAGE),
            Arguments.of(ProtobufGenerator.createDynamicProtobufRecord(), null),
            Arguments.of(ProtobufGenerator.createDynamicProtobufRecord(), ProtobufMessageType.UNKNOWN),
        )
    }
}
