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

package com.amazonaws.services.schemaregistry.flink.avro

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryCompressionHandler
import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDefaultCompression
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.avro.Schema
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.instanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryInputStreamDeserializerTest {
    @Mock
    private lateinit var mockDeserializer: GlueSchemaRegistryDeserializationFacade

    @BeforeEach
    fun setup() {
        metadata["test-key"] = "test-value"
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TEST_TOPIC

        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true

        userSchema = Schema.Parser().parse(File(AVRO_USER_SCHEMA_FILE))
        glueSchema =
            com.amazonaws.services.schemaregistry.common
                .Schema(userSchema.toString(), "Avro", TEST_TOPIC)
        userDefinedPojo =
            User
                .newBuilder()
                .setName("test_avro_schema")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build()
    }

    /**
     * Test whether constructor works with configuration map
     */
    @Test
    fun testConstructor_withConfigs_succeeds() {
        val deserializer = GlueSchemaRegistryInputStreamDeserializer(configs)
        assertThat(deserializer, instanceOf(GlueSchemaRegistryInputStreamDeserializer::class.java))
    }

    /**
     * Test whether constructor works with AWS de-serializer input
     */
    @Test
    fun testConstructor_withDeserializer_succeeds() {
        val deserializer = GlueSchemaRegistryInputStreamDeserializer(mockDeserializer)
        assertThat(deserializer, instanceOf(GlueSchemaRegistryInputStreamDeserializer::class.java))
    }

    /**
     * Test whether getSchemaAndDeserializedStream method works
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testGetSchemaAndDeserializedStream_withValidParams_succeeds(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        compressionByte =
            if (compressionType.name == "NONE") {
                AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE
            } else {
                AWSSchemaRegistryConstants.COMPRESSION_BYTE
            }
        glueSchemaRegistryCompressionHandler = GlueSchemaRegistryDefaultCompression()

        val byteArrayOutputStream =
            buildByteArrayOutputStream(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE, compressionByte)
        val encoded = encodeData(userDefinedPojo, SpecificDatumWriter(userSchema))
        val bytes =
            writeToExistingStream(
                byteArrayOutputStream,
                if (compressionType.name == "NONE") encoded else compressData(encoded),
            )

        val mutableByteArrayInputStream = MutableByteArrayInputStream()
        mutableByteArrayInputStream.setBuffer(bytes)

        whenever(mockDeserializer.getSchema(any())).thenReturn(glueSchema)
        whenever(mockDeserializer.getActualData(any())).thenReturn(bytes)
        val deserializer = GlueSchemaRegistryInputStreamDeserializer(mockDeserializer)
        val resultSchema = deserializer.getSchemaAndDeserializedStream(mutableByteArrayInputStream)

        assertThat(resultSchema.toString(), equalTo(glueSchema.schemaDefinition))
    }

    /**
     * Test whether getSchemaAndDeserializedStream method throws exception with invalid schema
     */
    @Test
    fun testGetSchemaAndDeserializedStream_withWrongSchema_throwsException() {
        val schemaDefinition =
            "{" +
                "\"type\":\"record\"," +
                "\"name\":\"User\"," +
                "\"namespace\":\"com.amazonaws.services.schemaregistry.serializers.avro\"," +
                "\"fields\":" +
                "[" +
                "{\"name\":\"name\",\"type\":\"string\"}," +
                "{\"name\":\"favorite_number\",\"name\":[\"int\",\"null\"]}," +
                "{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}" +
                "]" +
                "}"
        val mutableByteArrayInputStream = MutableByteArrayInputStream()
        glueSchema =
            com.amazonaws.services.schemaregistry.common
                .Schema(schemaDefinition, "Avro", TEST_TOPIC)

        whenever(mockDeserializer.getSchema(any())).thenReturn(glueSchema)
        whenever(mockDeserializer.getActualData(any())).thenReturn(ByteArray(0))
        val deserializer = GlueSchemaRegistryInputStreamDeserializer(mockDeserializer)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                deserializer.getSchemaAndDeserializedStream(mutableByteArrayInputStream)
            }
        assertThat(
            exception.message,
            equalTo("Error occurred while parsing schema, see inner exception for details."),
        )
    }

    @Throws(IOException::class)
    private fun buildByteArrayOutputStream(
        headerByte: Byte,
        compressionByte: Byte,
    ): ByteArrayOutputStream {
        val byteArrayOutputStream = ByteArrayOutputStream()
        byteArrayOutputStream.write(headerByte.toInt())
        byteArrayOutputStream.write(compressionByte.toInt())
        writeSchemaVersionId(byteArrayOutputStream)

        return byteArrayOutputStream
    }

    @Throws(IOException::class)
    private fun writeSchemaVersionId(out: ByteArrayOutputStream) {
        val buffer = ByteBuffer.wrap(ByteArray(AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE))
        buffer.putLong(USER_SCHEMA_VERSION_ID.mostSignificantBits)
        buffer.putLong(USER_SCHEMA_VERSION_ID.leastSignificantBits)
        out.write(buffer.array())
    }

    @Throws(IOException::class)
    private fun writeToExistingStream(
        toStream: ByteArrayOutputStream,
        fromStream: ByteArray,
    ): ByteArray {
        toStream.write(fromStream)
        return toStream.toByteArray()
    }

    @Throws(IOException::class)
    private fun encodeData(
        `object`: Any,
        writer: DatumWriter<Any>,
    ): ByteArray {
        val actualDataBytes = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().directBinaryEncoder(actualDataBytes, null)
        writer.write(`object`, encoder)
        encoder.flush()
        return actualDataBytes.toByteArray()
    }

    @Throws(IOException::class)
    private fun compressData(actualDataBytes: ByteArray): ByteArray = glueSchemaRegistryCompressionHandler.compress(actualDataBytes)

    companion object {
        private var compressionByte: Byte = 0
        private lateinit var userSchema: Schema
        private lateinit var glueSchema: com.amazonaws.services.schemaregistry.common.Schema
        private lateinit var userDefinedPojo: User
        private val configs: MutableMap<String, Any> = HashMap()
        private val metadata: MutableMap<String, String> = HashMap()
        private lateinit var glueSchemaRegistryCompressionHandler: GlueSchemaRegistryCompressionHandler

        private const val TEST_TOPIC = "Test-Topic"
        private val USER_SCHEMA_VERSION_ID: UUID = UUID.randomUUID()
        private const val AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc"
    }
}
