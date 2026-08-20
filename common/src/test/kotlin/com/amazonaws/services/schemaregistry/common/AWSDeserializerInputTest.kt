package com.amazonaws.services.schemaregistry.common

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class AWSDeserializerInputTest {
    @Test
    fun testBuilder_withByteBufferAndTransportName_objectBuildSuccessfully() {
        val byteBuffer = ByteBuffer.allocate(1)
        val awsDeserializerInput =
            AWSDeserializerInput
                .builder()
                .buffer(byteBuffer)
                .transportName(TRANSPORT_NAME)
                .build()

        assertEquals(TRANSPORT_NAME, awsDeserializerInput.transportName)
        assertEquals(byteBuffer, awsDeserializerInput.buffer)
    }

    @Test
    fun testBuilder_withByteBufferAndNullTransportName_objectBuildSuccessfully() {
        val byteBuffer = ByteBuffer.allocate(1)
        val awsDeserializerInput =
            AWSDeserializerInput
                .builder()
                .buffer(byteBuffer)
                .transportName(null)
                .build()

        assertEquals(DEFAULT_TRANSPORT_NAME, awsDeserializerInput.transportName)
        assertEquals(byteBuffer, awsDeserializerInput.buffer)
    }

    @Test
    fun testBuilder_nullByteBuffer_throwsException() {
        assertThrows(IllegalArgumentException::class.java) {
            AWSDeserializerInput
                .builder()
                .buffer(null)
                .transportName(TRANSPORT_NAME)
                .build()
        }
    }

    companion object {
        private const val TRANSPORT_NAME = "test-transport-name"
        private const val DEFAULT_TRANSPORT_NAME = "default-stream"
    }
}
