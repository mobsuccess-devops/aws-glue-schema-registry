package com.amazonaws.services.schemaregistry.deserializers.protobuf

import Foo.Contact
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
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
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.protobuf.Message
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ProtobufClassNameTest {
    @Test
    fun testFrom_OnNullDescriptor_ThrowsException() {
        assertThrows(NullPointerException::class.java) { ProtobufClassName.from(nullOf()) }
    }

    @ParameterizedTest
    @MethodSource("getPOJODecoderTestCases")
    fun testFrom_ConvertsFileDescriptorToClassNames_ForAllCases(
        message: Message,
        expectedClassName: Class<*>,
    ) {
        val messageDescriptor = message.descriptorForType

        val actualClassName = ProtobufClassName.from(messageDescriptor)
        assertEquals(expectedClassName.name, actualClassName)
        assertDoesNotThrow { Class.forName(actualClassName) }
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
            Arguments.of(ProtobufGenerator.HYPHEN_ATED_PROTO_FILE_MESSAGE, HyphenAtedProtoFile.hyphenated::class.java),
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
    }
}
