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

package com.amazonaws.services.schemaregistry.deserializers.protobuf

import com.amazonaws.services.schemaregistry.deserializers.PojoClassResolver
import com.amazonaws.services.schemaregistry.serializers.protobuf.MessageIndexFinder
import com.amazonaws.services.schemaregistry.utils.ProtobufMessageType
import com.google.protobuf.CodedInputStream
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import org.apache.commons.lang3.tuple.Pair
import java.io.IOException

class ProtobufWireFormatDecoder(
    private val messageIndexFinder: MessageIndexFinder,
) {
    @Throws(IOException::class)
    fun decode(
        data: ByteArray,
        descriptor: Descriptors.FileDescriptor,
        messageType: ProtobufMessageType?,
    ): Any {
        val indexAndStreamPair = getAndRemoveMessageIndex(data)
        val messageDescriptor = messageIndexFinder.getByIndex(descriptor, indexAndStreamPair.left)

        return if (ProtobufMessageType.POJO == messageType) {
            deserializeToPojo(messageDescriptor, indexAndStreamPair.right)
        } else {
            // Defaults to DynamicMessage if not set or set explicitly to DYNAMIC_MESSAGE.
            deserializeToDynamicMessage(messageDescriptor, indexAndStreamPair.right)
        }
    }

    /**
     * Deserialization method for DynamicMessage ProtobufMessageType.
     */
    @Throws(IOException::class)
    private fun deserializeToDynamicMessage(
        descriptor: Descriptors.Descriptor,
        codedInputStream: CodedInputStream,
    ): DynamicMessage = DynamicMessage.parseFrom(descriptor, codedInputStream)

    /**
     * Deserialization method for POJO ProtobufMessageType. Derives the class name from the message
     * descriptor and reflectively invokes it to deserialize the bytes into a POJO.
     */
    private fun deserializeToPojo(
        descriptor: Descriptors.Descriptor,
        codedInputStream: CodedInputStream,
    ): Any {
        val className = ProtobufClassName.from(descriptor)
        try {
            val classType = PojoClassResolver.resolve(className)
            val parseMethod = classType.getMethod("parseFrom", CodedInputStream::class.java)
            return parseMethod.invoke(classType, codedInputStream)
        } catch (e: Exception) {
            throw RuntimeException("Error de-serializing data into Message class: $className", e)
        }
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun getAndRemoveMessageIndex(data: ByteArray): Pair<Int, CodedInputStream> {
            val codedInputStream = CodedInputStream.newInstance(data)
            val messageIndex = codedInputStream.readUInt32()
            return Pair.of(messageIndex, codedInputStream)
        }
    }
}
