package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils
import com.google.protobuf.Descriptors
import com.squareup.wire.schema.internal.parser.ProtoParser
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

class ProtobufTestCase {
    var fileName: String? = null

    fun getRawSchema(): String = try {
        String(
            Files.readAllBytes(Paths.get(ProtobufTestCaseReader.TEST_PROTO_PATH, fileName)),
            StandardCharsets.UTF_8,
        )
    } catch (e: IOException) {
        throw RuntimeException("Error reading file", e)
    }

    fun getPackage(): String = getSchema().`package`

    fun getSchema(): Descriptors.FileDescriptor {
        val rawSchema = this.getRawSchema()
        val fileElem = ProtoParser.parse(FileDescriptorUtils.DEFAULT_LOCATION, rawSchema)
        try {
            return FileDescriptorUtils.protoFileToFileDescriptor(fileElem)
        } catch (e: Descriptors.DescriptorValidationException) {
            throw RuntimeException("Error parsing descriptors from Protobuf schema", e)
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ProtobufTestCase) return false
        return fileName == other.fileName
    }

    override fun hashCode(): Int = fileName?.hashCode() ?: 0

    override fun toString(): String = "ProtobufTestCase(fileName=$fileName)"
}
