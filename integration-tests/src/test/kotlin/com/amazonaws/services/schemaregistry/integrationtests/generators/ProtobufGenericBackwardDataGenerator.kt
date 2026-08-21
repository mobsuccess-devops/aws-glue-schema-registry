package com.amazonaws.services.schemaregistry.integrationtests.generators

import com.amazonaws.services.schemaregistry.deserializers.protobuf.ProtobufSchemaParser
import com.amazonaws.services.schemaregistry.serializers.protobuf.ProtobufGenerator
import com.google.common.base.Charsets
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import java.nio.file.Files
import java.nio.file.Paths

class ProtobufGenericBackwardDataGenerator : TestDataGenerator<DynamicMessage> {
    override fun createRecords(): List<DynamicMessage> {
        // Refer to protobufBackwardTestV1.proto
        val messageName = "AllTypes"
        val baseMessage = ProtobufGenerator.ALL_TYPES_MESSAGE_SYNTAX2
        val v1SchemaFileDescriptor =
            ProtobufSchemaParser.parse(getSchemaDef("protobufBackwardTestV1.proto"), "protobufBackwardTest.proto")
        val v2SchemaFileDescriptor =
            ProtobufSchemaParser.parse(getSchemaDef("protobufBackwardTestV2.proto"), "protobufBackwardTest.proto")

        val v1DynamicMessage1 =
            DynamicMessage
                .newBuilder(v1SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(123)
                        .setStringType(ALLOWLISTED)
                        .build()
                        .toByteArray(),
                ).build()
        val v2DynamicMessage1 =
            DynamicMessage
                .newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(456)
                        .setStringType(ALLOWLISTED)
                        .build()
                        .toByteArray(),
                ).build()

        val v1DynamicMessage2 =
            DynamicMessage
                .newBuilder(v1SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setInt32Type(56)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray(),
                ).build()

        val v2DynamicMessage2 =
            DynamicMessage
                .newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setFixed32Type(56024)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray(),
                ).build()
        val v2DynamicMessage3 =
            DynamicMessage
                .newBuilder(v2SchemaFileDescriptor.findMessageTypeByName(messageName))
                .mergeFrom(
                    baseMessage
                        .toBuilder()
                        .setFixed32Type(24)
                        .setStringType(BLOCKLISTED)
                        .build()
                        .toByteArray(),
                ).build()

        return listOf(
            v1DynamicMessage1,
            v2DynamicMessage2,
            v1DynamicMessage2,
            v2DynamicMessage1,
            v2DynamicMessage3,
        )
    }

    private fun getSchemaDef(schemaName: String): String {
        val schemaFilePath = Paths.get("src/test/resources/protobuf/$schemaName")
        return String(Files.readAllBytes(schemaFilePath), Charsets.UTF_8)
    }

    companion object {
        private const val ALLOWLISTED = "allowListed"
        private const val BLOCKLISTED = "blockListed"

        @JvmStatic
        fun filterRecords(value: Message): Boolean {
            val message = value as DynamicMessage
            val nameField =
                message.allFields.keys
                    .stream()
                    .filter { fd -> fd.name == "stringType" }
                    .findFirst()
            val name = message.getField(nameField.get()) as String
            return name == ALLOWLISTED
        }
    }
}
