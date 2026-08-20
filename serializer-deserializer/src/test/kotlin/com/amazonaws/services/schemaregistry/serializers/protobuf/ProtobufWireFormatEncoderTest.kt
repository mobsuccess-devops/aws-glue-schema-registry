/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates.
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

import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic
import com.amazonaws.services.schemaregistry.utils.nullOf
import com.google.protobuf.CodedInputStream
import com.google.protobuf.DynamicMessage
import com.google.protobuf.Message
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ProtobufWireFormatEncoderTest {
    @Test
    fun testEncode_WhenNullsArePassed_ThrowsException() {
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_WIRE_FORMAT.encode(nullOf(), CUSTOMER_FILE_DESCRIPTOR)
        }

        assertThrows(NullPointerException::class.java) {
            PROTOBUF_WIRE_FORMAT.encode(CUSTOMER_MESSAGE, nullOf())
        }
    }

    @Test
    fun testPrefixMessageIndexToBytes_WhenNullsArePassed_ThrowsException() {
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(
                nullOf(),
                CUSTOMER_FILE_DESCRIPTOR,
                CUSTOMER_MESSAGE.descriptorForType,
            )
        }
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(
                ByteArray(0),
                nullOf(),
                CUSTOMER_MESSAGE.descriptorForType,
            )
        }
        assertThrows(NullPointerException::class.java) {
            PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(ByteArray(0), CUSTOMER_FILE_DESCRIPTOR, nullOf())
        }
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    fun testEncode_EncodesMessageAndMessageIndex_SuccessfullyDecodesToPOJO(message: Message) {
        val encodedMessage = PROTOBUF_WIRE_FORMAT.encode(message, CUSTOMER_FILE_DESCRIPTOR)

        val codedInputStream = CodedInputStream.newInstance(encodedMessage)

        val actualMessageIndex = codedInputStream.readUInt32()

        assertEquals(MESSAGE_INDEX, actualMessageIndex)

        val actualCustomerMessage = Basic.Customer.parseFrom(codedInputStream)

        assertEquals(CUSTOMER_MESSAGE, actualCustomerMessage)
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    fun testEncode_EncodesMessageAndMessageIndex_SuccessfullyDecodesToDynamicMessage(message: Message) {
        val encodedMessage = PROTOBUF_WIRE_FORMAT.encode(message, CUSTOMER_FILE_DESCRIPTOR)

        val codedInputStream = CodedInputStream.newInstance(encodedMessage)

        val actualMessageIndex = codedInputStream.readUInt32()

        assertEquals(MESSAGE_INDEX, actualMessageIndex)

        val actualDynamicCustomerMessage =
            DynamicMessage.parseFrom(Basic.Customer.getDescriptor(), codedInputStream)

        assertEquals(DYNAMIC_CUSTOMER_MESSAGE, actualDynamicCustomerMessage)
    }

    @ParameterizedTest
    @MethodSource("testMessageProvider")
    fun testPrefixMessageIndexToBytes_SuccessfullyPrefixesCorrectMessageIndex(message: Message) {
        val messageBytes = message.toByteArray()
        val prefixedBytes =
            PROTOBUF_WIRE_FORMAT.prefixMessageIndexToBytes(
                messageBytes,
                CUSTOMER_FILE_DESCRIPTOR,
                message.descriptorForType,
            )
        val codedInputStream = CodedInputStream.newInstance(prefixedBytes)

        val actualMessageIndex = codedInputStream.readUInt32()

        assertEquals(MESSAGE_INDEX, actualMessageIndex)
    }

    companion object {
        private const val MESSAGE_INDEX = 1

        private val PROTOBUF_WIRE_FORMAT = ProtobufWireFormatEncoder(MessageIndexFinder())

        private const val NAME = "Foo"
        private const val NAME_FIELD = "name"
        private val CUSTOMER_MESSAGE: Basic.Customer = Basic.Customer.newBuilder().setName(NAME).build()
        private val CUSTOMER_FILE_DESCRIPTOR = Basic.Customer.getDescriptor().file
        private val DYNAMIC_CUSTOMER_MESSAGE: DynamicMessage =
            DynamicMessage
                .newBuilder(Basic.Customer.getDescriptor())
                .setField(Basic.Customer.getDescriptor().findFieldByName(NAME_FIELD), NAME)
                .build()

        @JvmStatic
        fun testMessageProvider(): Stream<Arguments> = Stream.of(CUSTOMER_MESSAGE, DYNAMIC_CUSTOMER_MESSAGE).map { Arguments.of(it) }
    }
}
