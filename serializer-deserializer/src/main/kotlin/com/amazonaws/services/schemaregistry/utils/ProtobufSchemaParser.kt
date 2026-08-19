package com.amazonaws.services.schemaregistry.utils

import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils
import com.google.protobuf.DescriptorProtos

object ProtobufSchemaParser {
    private const val RAW_SCHEMA_HEADER = "// Proto schema formatted by Wire, do not edit.\n// Source: \n\n"

    /**
     * Get the Protobuf schema definition string from FileDescriptorProto object.
     */
    @JvmStatic
    fun getProtobufSchemaStringFromFileDescriptorProto(
        fileDescriptorProto: DescriptorProtos.FileDescriptorProto,
    ): String {
        val rawSchema = FileDescriptorUtils.fileDescriptorToProtoFile(fileDescriptorProto).toSchema()
        return rawSchema.replace(RAW_SCHEMA_HEADER, "")
    }
}
