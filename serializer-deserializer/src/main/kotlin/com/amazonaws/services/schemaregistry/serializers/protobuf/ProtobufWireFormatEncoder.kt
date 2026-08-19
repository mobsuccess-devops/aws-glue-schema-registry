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

import com.google.protobuf.CodedOutputStream
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.UncheckedIOException

/**
 * Writes the wire format for Schema Registry embedded Protobuf messages.
 */
class ProtobufWireFormatEncoder(
    private val messageIndexFinder: MessageIndexFinder,
) {
    /**
     * Encodes the message index as a zig-zag encoded variable size int into Byte stream.
     */
    fun encode(
        message: Message,
        schemaFileDescriptor: Descriptors.FileDescriptor,
    ): ByteArray {
        val descriptor = message.descriptorForType
        try {
            return prefixMessageIndexToBytes(message.toByteArray(), schemaFileDescriptor, descriptor)
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }
    }

    @Throws(IOException::class)
    fun prefixMessageIndexToBytes(
        bytesToEncode: ByteArray,
        schemaFileDescriptor: Descriptors.FileDescriptor,
        fileDescriptor: Descriptors.Descriptor,
    ): ByteArray {
        val outputStream = ByteArrayOutputStream()
        val codedOutputStream = CodedOutputStream.newInstance(outputStream)

        val messageIndex = messageIndexFinder.getByDescriptor(schemaFileDescriptor, fileDescriptor)
        codedOutputStream.writeUInt32NoTag(messageIndex)
        codedOutputStream.writeRawBytes(bytesToEncode)
        codedOutputStream.flush()

        return outputStream.toByteArray()
    }
}
