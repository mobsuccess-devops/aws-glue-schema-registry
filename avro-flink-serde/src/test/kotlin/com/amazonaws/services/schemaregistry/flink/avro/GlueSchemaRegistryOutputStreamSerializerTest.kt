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

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.avro.Schema
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.instanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.io.ByteArrayOutputStream
import java.io.File

@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryOutputStreamSerializerTest {
    @Mock
    private lateinit var mockSerializationFacade: GlueSchemaRegistrySerializationFacade

    @BeforeEach
    fun setup() {
        metadata["test-key"] = "test-value"
        metadata[AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY] = TEST_TOPIC

        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true

        userSchema = Schema.Parser().parse(File(AVRO_USER_SCHEMA_FILE))
        userDefinedPojo =
            User
                .newBuilder()
                .setName("test_avro_schema")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build()
    }

    /**
     * Test whether constructor works with topic name and AWS Glue Schema Registry configuration map
     */
    @Test
    fun testConstructor_withConfigsAndCredential_succeeds() {
        val serializer = GlueSchemaRegistryOutputStreamSerializer(TEST_TOPIC, configs)
        assertThat(serializer, instanceOf(GlueSchemaRegistryOutputStreamSerializer::class.java))
    }

    /**
     * Test whether constructor works with Glue Schema Registry SerializationFacade
     */
    @Test
    fun testConstructor_withDeserializer_succeeds() {
        val serializer = GlueSchemaRegistryOutputStreamSerializer(TEST_TOPIC, configs, mockSerializationFacade)
        assertThat(serializer, instanceOf(GlueSchemaRegistryOutputStreamSerializer::class.java))
    }

    /**
     * Test whether registerSchemaAndSerializeStream method works
     */
    @Test
    fun testRegisterSchemaAndSerializeStream_withValidParams_succeeds() {
        whenever(
            mockSerializationFacade.encode(
                any<String>(),
                any<com.amazonaws.services.schemaregistry.common.Schema>(),
                any(),
            ),
        ).thenReturn(specificBytes)
        val serializer = GlueSchemaRegistryOutputStreamSerializer(TEST_TOPIC, configs, mockSerializationFacade)
        val outputStream = ByteArrayOutputStream()
        serializer.registerSchemaAndSerializeStream(userSchema, outputStream, actualBytes)

        assertThat(outputStream.toByteArray(), equalTo(specificBytes))
    }

    companion object {
        private lateinit var userSchema: Schema
        private lateinit var userDefinedPojo: User
        private val configs: MutableMap<String, Any> = HashMap()
        private val metadata: MutableMap<String, String> = HashMap()

        private const val TEST_TOPIC = "Test-Topic"
        private const val AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc"
        private val actualBytes =
            byteArrayOf(12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116)
        private val specificBytes =
            byteArrayOf(
                3, 0, -73, -76, -89, -16, -100, -106, 78, 74, -90, -121, -5,
                93, -23, -17, 12, 99, 8, 116, 101, 115, 116, 0, 20, 0, 12, 118, 105, 111, 108, 101, 116,
            )
    }
}
