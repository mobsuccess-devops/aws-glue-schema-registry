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

import com.amazonaws.services.schemaregistry.common.AWSDeserializerInput
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

/**
 * Unit tests for testing Kafka specific de-serializer.
 */
@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryKafkaDeserializerTest {
    private val configs: MutableMap<String, Any> = HashMap()

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    @BeforeEach
    fun setup() {
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = "NONE"
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer instantiation.
     */
    @Test
    fun test_Create() {
        // Test create with empty constructor
        val glueSchemaRegistryKafkaDeserializer1 = GlueSchemaRegistryKafkaDeserializer()
        assertNotNull(glueSchemaRegistryKafkaDeserializer1.credentialProvider)

        // Test create with AWSCredentialsProvider constructor
        val glueSchemaRegistryKafkaDeserializer2 =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, configs)
        assertNotNull(glueSchemaRegistryKafkaDeserializer2.credentialProvider)
    }

    @Test
    fun test_Create_With_Aws_Deserializer() {
        // Test create with empty constructor
        val glueSchemaRegistryKafkaDeserializer =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null)
        glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade =
            GlueSchemaRegistryDeserializationFacade
                .builder()
                .credentialProvider(this.mockCredProvider)
                .configs(configs)
                .build()

        assertNotNull(glueSchemaRegistryKafkaDeserializer.credentialProvider)
        assertNotNull(glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade)
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for empty configuration.
     */
    @Test
    fun test_Configure_Empty_Config() {
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()
        assertNotNull(glueSchemaRegistryKafkaDeserializer.credentialProvider)

        val configs = HashMap<String, Any>()
        assertThrows(IllegalArgumentException::class.java) {
            glueSchemaRegistryKafkaDeserializer.configure(configs, false)
        }
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    fun test_Deserialize_Null_Input() {
        // Mock the dependency
        val glueSchemaRegistryDeserializationFacade = mock<GlueSchemaRegistryDeserializationFacade>()
        val glueSchemaRegistryKafkaDeserializer =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null)
        glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade =
            glueSchemaRegistryDeserializationFacade

        val result = glueSchemaRegistryKafkaDeserializer.deserialize("TestTopic", null)
        assertNull(result)
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer deserialize method by mocking the dependency.
     */
    @Test
    fun test_Deserialize() {
        val expectedObject = Any()
        // Mock the dependency
        val glueSchemaRegistryDeserializationFacade = mock<GlueSchemaRegistryDeserializationFacade>()
        whenever(glueSchemaRegistryDeserializationFacade.deserialize(any<AWSDeserializerInput>()))
            .thenReturn(expectedObject)
        val glueSchemaRegistryKafkaDeserializer =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null)
        glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade =
            glueSchemaRegistryDeserializationFacade

        val deserializedObject =
            glueSchemaRegistryKafkaDeserializer.deserialize(
                "TestTopic",
                byteArrayOf(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE),
            )
        assertEquals(expectedObject, deserializedObject)
    }

    /**
     * Tests invoking shutdown invokes the internal AWSDeserializer.close method.
     */
    @Test
    fun testClose_callInternalAWSDeserializer_succeeds() {
        val glueSchemaRegistryDeserializationFacade = mock<GlueSchemaRegistryDeserializationFacade>()
        val glueSchemaRegistryKafkaDeserializer =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, null)
        glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade =
            glueSchemaRegistryDeserializationFacade

        Mockito.verify(glueSchemaRegistryDeserializationFacade, Mockito.atMost(1)).close()
    }

    @Test
    fun testDeserialize_beforeConfigure_saysConfigureWasNotCalled() {
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()

        val thrown =
            assertThrows(IllegalStateException::class.java) {
                glueSchemaRegistryKafkaDeserializer.deserialize("User-Topic", byteArrayOf(3, 0, 1, 2))
            }

        assertTrue(thrown.message!!.contains("configure()"), thrown.message)
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for null pointer exception by passing null config
     */
    @Test
    fun testConfigure_nullConfig_throwsException() {
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()
        assertNotNull(glueSchemaRegistryKafkaDeserializer.credentialProvider)

        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryKafkaDeserializer.configure(nullOf(), false)
        }
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer configure method for positive scenario by passing valid config.
     */
    @Test
    fun testConfigure_validConfig_throwsException() {
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()
        assertNotNull(glueSchemaRegistryKafkaDeserializer.credentialProvider)

        assertDoesNotThrow { glueSchemaRegistryKafkaDeserializer.configure(configs, false) }
    }

    /**
     * Test GlueSchemaRegistryKafkaDeserializer constructor for null pointer exception by passing null config.
     */
    @Test
    fun testConstructor_nullConfig_throwsException() {
        assertThrows(NullPointerException::class.java) { GlueSchemaRegistryKafkaDeserializer(nullOf()) }
    }

    /**
     * Tests invoking close method.
     */
    @Test
    fun testClose_succeeds() {
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()
        glueSchemaRegistryKafkaDeserializer.configure(configs, false)

        assertDoesNotThrow { glueSchemaRegistryKafkaDeserializer.close() }
    }
}
