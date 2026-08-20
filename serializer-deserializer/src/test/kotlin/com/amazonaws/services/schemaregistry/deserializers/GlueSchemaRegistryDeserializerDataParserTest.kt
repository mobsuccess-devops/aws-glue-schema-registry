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

package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.platform.commons.util.ReflectionUtils
import java.nio.ByteBuffer
import java.util.UUID
import java.util.stream.Stream

class GlueSchemaRegistryDeserializerDataParserTest {
    /**
     * Tests the isDataCompatible for failure case where the compression byte is unknown.
     */
    @Test
    fun test_InvalidHeader_ThrowsAWSIncompatibleDataException() {
        val serializedData =
            constructSerializedData(99, AWSSchemaRegistryConstants.COMPRESSION_BYTE, UUID.randomUUID())
        val exception =
            assertThrows(GlueSchemaRegistryIncompatibleDataException::class.java) {
                GlueSchemaRegistryDeserializerDataParser
                    .getInstance()
                    .getSchemaVersionId(ByteBuffer.wrap(serializedData))
            }
        assertEquals(
            GlueSchemaRegistryIncompatibleDataException.UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE,
            exception.message,
        )
    }

    /**
     * Tests the isDataCompatible for failure case where the header version byte is unknown.
     */
    @Test
    fun test_Invalid_Compression_Byte() {
        val serializedData =
            constructSerializedData(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, 99, UUID.randomUUID())
        val errorBuilder = StringBuilder()
        assertFalse(
            GlueSchemaRegistryDeserializerDataParser
                .getInstance()
                .isDataCompatible(ByteBuffer.wrap(serializedData), errorBuilder),
        )
        assertEquals(
            GlueSchemaRegistryIncompatibleDataException.UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE,
            errorBuilder.toString(),
        )
    }

    /**
     * Tests the when the buffer length is invalid.
     */
    @Test
    fun test_Invalid_Length() {
        val errorBuilder = StringBuilder()
        assertFalse(
            GlueSchemaRegistryDeserializerDataParser
                .getInstance()
                .isDataCompatible(ByteBuffer.wrap(ByteArray(2)), errorBuilder),
        )
        assertTrue(
            errorBuilder
                .toString()
                .contains(GlueSchemaRegistryIncompatibleDataException.UNKNOWN_DATA_ERROR_MESSAGE),
        )
    }

    /**
     * Ensure validation doesn't leave the bytebuffer at random position.
     */
    @ParameterizedTest
    @MethodSource("testValidateBuffersProvider")
    fun test_Validate_RetainsBuffersInitialPosition(buffer: ByteBuffer) {
        val initialBytePosition = buffer.position()

        val errorBuilder = StringBuilder()

        GlueSchemaRegistryDeserializerDataParser.getInstance().isDataCompatible(buffer, errorBuilder)

        val currentPosition = buffer.position()

        assertEquals(initialBytePosition, currentPosition)
    }

    @ParameterizedTest
    @MethodSource("testAWSDeserializeDataParserMethods")
    fun test_DataParserMethods_RetainBuffersInitialPosition(methodName: String) {
        val method =
            ReflectionUtils.findMethods(GlueSchemaRegistryDeserializerDataParser::class.java) { m ->
                m.name == methodName
            }

        assertTrue(method.size > 0, "Method $methodName doesn't exist")

        val glueSchemaRegistryDeserializerDataParser = GlueSchemaRegistryDeserializerDataParser.getInstance()

        val validSchemaRegistryData =
            ByteBuffer.wrap(
                constructSerializedData(
                    AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                    AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE,
                    UUID.randomUUID(),
                ),
            )

        val initialBytePosition = validSchemaRegistryData.position()

        method[0].invoke(glueSchemaRegistryDeserializerDataParser, validSchemaRegistryData)

        val currentPosition = validSchemaRegistryData.position()
        assertEquals(initialBytePosition, currentPosition, "Assertion failed for $methodName")
    }

    /**
     * Tests the isDataCompatible for success case where the header version byte is unknown.
     */
    @Test
    fun test_Success() {
        val errorBuilder = StringBuilder()
        val serializedData =
            constructSerializedData(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                UUID.randomUUID(),
            )
        assertTrue(
            GlueSchemaRegistryDeserializerDataParser
                .getInstance()
                .isDataCompatible(ByteBuffer.wrap(serializedData), errorBuilder),
        )
    }

    companion object {
        /**
         * Helper method to construct a serialized message from the supplied byte parameters with UUID.
         *
         * @param headerVersionByte header version byte for schema registry
         * @param compressionByte   compression byte for schema registry
         * @param uuid              schema version id
         * @return constructed byte array of the message
         */
        private fun constructSerializedData(
            headerVersionByte: Byte,
            compressionByte: Byte,
            uuid: UUID,
        ): ByteArray {
            val byteBuffer = ByteBuffer.wrap(ByteArray(18))

            byteBuffer.put(headerVersionByte)
            byteBuffer.put(compressionByte)
            byteBuffer.putLong(uuid.mostSignificantBits)
            byteBuffer.putLong(uuid.leastSignificantBits)

            return byteBuffer.array()
        }

        @JvmStatic
        fun testAWSDeserializeDataParserMethods(): Stream<Arguments> = setOf(
            "getPlainData",
            "getSchemaVersionId",
            "isCompressionEnabled",
            "getCompressionByte",
            "getHeaderVersionByte",
        ).stream()
            .map { Arguments.of(it) }

        @JvmStatic
        fun testValidateBuffersProvider(): Stream<Arguments> {
            val randomInvalidByte: Byte = 90
            val invalidSchemaRegistryData =
                ByteBuffer.wrap(
                    constructSerializedData(
                        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                        randomInvalidByte,
                        UUID.randomUUID(),
                    ),
                )

            val validSchemaRegistryData =
                ByteBuffer.wrap(
                    constructSerializedData(
                        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                        AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                        UUID.randomUUID(),
                    ),
                )

            return setOf(
                invalidSchemaRegistryData,
                validSchemaRegistryData,
            ).stream()
                .map { Arguments.of(it) }
        }
    }
}
