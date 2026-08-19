package com.amazonaws.services.schemaregistry.deserializers.protobuf

import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils
import com.google.protobuf.Descriptors
import com.squareup.wire.schema.internal.parser.ProtoParser

/**
 * Utility class to parse the Protobuf schemas using square and apicurio library.
 */
object ProtobufSchemaParser {
    @JvmStatic
    @Throws(Descriptors.DescriptorValidationException::class)
    fun parse(
        schemaDefinition: String,
        protoFileName: String,
    ): Descriptors.FileDescriptor {
        val fileElement = ProtoParser.parse(FileDescriptorUtils.DEFAULT_LOCATION, schemaDefinition)
        return FileDescriptorUtils.protoFileToFileDescriptor(fileElement, protoFileName)
    }
}
