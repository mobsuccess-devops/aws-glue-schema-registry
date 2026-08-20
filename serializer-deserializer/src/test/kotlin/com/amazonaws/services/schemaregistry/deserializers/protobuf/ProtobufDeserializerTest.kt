package com.amazonaws.services.schemaregistry.deserializers.protobuf

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.SerializationDataEncoder
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufSerializer
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufTestCaseReader
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.stream.Stream

class ProtobufDeserializerTest {
    private val protobufDynamicMessageDeserializer = ProtobufDeserializer(dynamicMessageConfigs)
    private val protobufPojoMessageTypeDeserializer = ProtobufDeserializer(pojoMessageConfigs)

    @Test
    fun testBuilder_Succeeds() {
        val deserializer =
            ProtobufDeserializer
                .builder()
                .configs(dynamicMessageConfigs)
                .build()
        assertNotNull(deserializer)
    }

    @Test
    fun testDeserialize_NullArgs_ThrowsException() {
        val dynamicMessage = ProtobufGenerator.createDynamicProtobufRecord()
        val buffer =
            ByteBuffer.wrap(
                SERIALIZATION_DATA_ENCODER.write(
                    protobufSerializer.serialize(dynamicMessage),
                    SCHEMA_VERSION_ID_FOR_TESTING,
                ),
            )
        val schema = ProtobufTestCaseReader.getTestCaseByName("Basic.proto").getRawSchema()

        val schemaObject = Schema(schema, DataFormat.PROTOBUF.name, SCHEMA_NAME)

        var ex: Exception =
            assertThrows(NullPointerException::class.java) {
                protobufDynamicMessageDeserializer.deserialize(nullOf(), schemaObject)
            }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services." +
                "schemaregistry.deserializers.protobuf.ProtobufDeserializer.deserialize, parameter data",
            ex.message,
        )

        ex =
            assertThrows(NullPointerException::class.java) {
                protobufDynamicMessageDeserializer.deserialize(buffer, nullOf())
            }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services." +
                "schemaregistry.deserializers.protobuf.ProtobufDeserializer.deserialize, parameter schema",
            ex.message,
        )
    }

    @ParameterizedTest
    @MethodSource("testDeserializationMessageProvider")
    fun testDeserialize_DynamicMessage_Succeeds(
        message: Message,
        schemaDef: String,
        schemaName: String,
    ) {
        val serializedData = protobufSerializer.serialize(message)
        val schemaObject = Schema(schemaDef, DataFormat.PROTOBUF.name, schemaName)
        val byteBuffer =
            ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(serializedData, SCHEMA_VERSION_ID_FOR_TESTING))

        val deserializedObject =
            protobufDynamicMessageDeserializer.deserialize(byteBuffer, schemaObject) as DynamicMessage

        assertArrayEquals(message.toByteArray(), deserializedObject.toByteArray())
    }

    @ParameterizedTest
    @MethodSource("testDeserializationMessageProvider")
    fun testDeserialize_WhenDeserializedToPOJO_Succeeds(
        message: Message,
        schemaDef: String,
        schemaName: String,
    ) {
        val serializedData = protobufSerializer.serialize(message)
        val schemaObject = Schema(schemaDef, DataFormat.PROTOBUF.name, schemaName)
        val byteBuffer =
            ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(serializedData, SCHEMA_VERSION_ID_FOR_TESTING))

        val deserializedObject =
            protobufPojoMessageTypeDeserializer.deserialize(byteBuffer, schemaObject) as Message

        assertArrayEquals(message.toByteArray(), deserializedObject.toByteArray())
    }

    @Test
    fun testDeserialize_DynamicMessage_ThrowsExceptionInvalidSchema() {
        val ex =
            assertThrows(NullPointerException::class.java) {
                protobufDynamicMessageDeserializer.deserialize(ANY_BUFFER, nullOf())
            }
        assertEquals(
            "Parameter specified as non-null is null: method com.amazonaws.services." +
                "schemaregistry.deserializers.protobuf.ProtobufDeserializer.deserialize, parameter schema",
            ex.message,
        )
    }

    @Test
    fun testDeserialize_DynamicMessage_ThrowsExceptionInvalidBytes() {
        val random = "invalid bytes"
        val invalidBytes = ByteBuffer.wrap(random.toByteArray(StandardCharsets.UTF_8))
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                protobufDynamicMessageDeserializer.deserialize(invalidBytes, ANY_SCHEMA)
            }
        assertEquals("Exception occurred while de-serializing Protobuf message", ex.message)
    }

    @Test
    fun testDeserialize_WhenDeserializeIsCalled_ReturnsCachedInstance() {
        // Test cases consist of same messages present twice in mixed order.
        val testCases = testDeserializeCacheMessageProvider()

        for (testCase in testCases) {
            val arguments = testCase.get()
            val serializedData = protobufSerializer.serialize(arguments[0])
            val schemaObject =
                Schema(arguments[1] as String, DataFormat.PROTOBUF.name, arguments[2] as String)
            val byteBuffer =
                ByteBuffer.wrap(SERIALIZATION_DATA_ENCODER.write(serializedData, SCHEMA_VERSION_ID_FOR_TESTING))

            // Call de-serialize repeatedly.
            protobufDynamicMessageDeserializer.deserialize(byteBuffer, schemaObject)
        }

        assertEquals(3, protobufDynamicMessageDeserializer.schemaParserCache.size())
    }

    companion object {
        const val SCHEMA_NAME = "Basic"
        private val ANY_BUFFER: ByteBuffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        private val ANY_SCHEMA = Schema("foo", DataFormat.PROTOBUF.name, SCHEMA_NAME)

        private val dynamicMessageConfigs =
            GlueSchemaRegistryConfiguration(
                mapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
                    AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE to "DYNAMIC_MESSAGE",
                ),
            )

        private val pojoMessageConfigs =
            GlueSchemaRegistryConfiguration(
                mapOf(
                    AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
                    AWSSchemaRegistryConstants.PROTOBUF_MESSAGE_TYPE to "POJO",
                ),
            )

        private val protobufSerializer = ProtobufSerializer(dynamicMessageConfigs)
        private val SERIALIZATION_DATA_ENCODER = SerializationDataEncoder(dynamicMessageConfigs)
        private val SCHEMA_VERSION_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")

        @JvmStatic
        fun testDeserializationMessageProvider(): Stream<Arguments> = Stream.of(
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
                ProtobufGenerator.ALL_TYPES_MESSAGE_SYNTAX3,
                ProtobufTestCaseReader.getTestCaseByName("AllTypesSyntax3.proto").getRawSchema(),
                "AllTypesSyntax3",
            ),
            Arguments.of(
                ProtobufGenerator.ALL_TYPES_MESSAGE_SYNTAX2,
                ProtobufTestCaseReader.getTestCaseByName("AllTypesSyntax2.proto").getRawSchema(),
                "allTypesSyntax2",
            ),
        )

        @JvmStatic
        fun testDeserializeCacheMessageProvider(): List<Arguments> = listOf(
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO2,
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax2.proto").getRawSchema(),
                "ComplexNestingSyntax2.proto",
            ),
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
                ProtobufGenerator.SNAKE_CASE_MESSAGE,
                ProtobufTestCaseReader.getTestCaseByName("snake_case_file.proto").getRawSchema(),
                "snake_case_file.proto",
            ),
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO3,
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax3.proto").getRawSchema(),
                "ComplexNestingSyntax3.proto",
            ),
            Arguments.of(
                ProtobufGenerator.NESTING_MESSAGE_PROTO3,
                ProtobufTestCaseReader.getTestCaseByName("ComplexNestingSyntax3.proto").getRawSchema(),
                "ComplexNestingSyntax3.proto",
            ),
        )
    }
}
