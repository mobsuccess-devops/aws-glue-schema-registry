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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

@ExtendWith(MockitoExtension::class)
class SecondaryDeserializerTest {
    private val configs: MutableMap<String, Any> = HashMap()
    private lateinit var secondaryDeserializer: SecondaryDeserializer
    private lateinit var schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration

    @Mock
    private lateinit var mockCredProvider: AwsCredentialsProvider

    @Mock
    private lateinit var mockGlueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    fun setup() {
        secondaryDeserializer = SecondaryDeserializer.newInstance()

        this.configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        this.configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        this.schemaRegistrySerDeConfigs = GlueSchemaRegistryConfiguration(this.configs)
    }

    @Test
    fun testDeserialize_nullObjField_throwsException() {
        val objField = SecondaryDeserializer::class.java.getDeclaredField("obj")
        objField.isAccessible = true
        objField.set(secondaryDeserializer, null)

        assertThrows(AWSSchemaRegistryException::class.java) { secondaryDeserializer.deserialize(null, null) }
    }

    @Test
    fun testClose_nullObjField_throwsException() {
        val objField = SecondaryDeserializer::class.java.getDeclaredField("obj")
        objField.isAccessible = true
        objField.set(secondaryDeserializer, null)

        assertThrows(AWSSchemaRegistryException::class.java) { secondaryDeserializer.close() }
    }

    /**
     * Tests the SecondaryDeserializer by importing a valid third party kafka deserializer
     */
    @Test
    fun testSecondaryDeserializer_validDeserializer_deserializesSuccessfully() {
        val serializedBytes = byteArrayOf(0)

        val configs = getConfigsWithSecondaryDeserializer(VALID_SECONDARY_DESERIALIZER)
        val deserializedObject = deserialize(configs, serializedBytes)

        assertNotNull(deserializedObject)
        assertTrue(deserializedObject is Any)
    }

    /**
     * Tests the SecondaryDeserializer by importing a String kafka deserializer
     */
    @Test
    fun testSecondaryDeserializer_withStringDeserializer_deserializesSuccessfully() {
        val stringSerializer = StringSerializer()
        val objectToSerialize = "TestJsonRecord"
        val serializedBytes = stringSerializer.serialize(TEST_TOPIC, objectToSerialize)

        val deserializerConfigs = getConfigsWithSecondaryDeserializer(StringDeserializer::class.java.name)
        val deserializedObject = deserialize(deserializerConfigs, serializedBytes)

        assertNotNull(deserializedObject)
        assertTrue(deserializedObject is String)
        assertEquals(objectToSerialize, deserializedObject)
    }

    /**
     * Tests the SecondaryDeserializer by importing a Integer kafka deserializer
     */
    @Test
    fun testSecondaryDeserializer_withIntegerDeserializer_deserializesSuccessfully() {
        val integerSerializer = IntegerSerializer()
        val objectToSerialize = 1
        val serializedBytes = integerSerializer.serialize(TEST_TOPIC, objectToSerialize)

        val deserializerConfigs = getConfigsWithSecondaryDeserializer(IntegerDeserializer::class.java.name)
        val deserializedObject = deserialize(deserializerConfigs, serializedBytes)

        assertNotNull(deserializedObject)
        assertTrue(deserializedObject is Int)
        assertEquals(objectToSerialize, deserializedObject)
    }

    /**
     * Tests the SecondaryDeserializer by importing deserializer not from Kafka - negative case.
     */
    @Test
    fun testSecondaryDeserializer_invalidDeserializer_throwsException() {
        val configs = getConfigsWithSecondaryDeserializer(NON_KAFKA_SECONDARY_DESERIALIZER)
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                glueSchemaRegistryKafkaDeserializer.configure(configs, true)
            }

        assertEquals(NON_KAFKA_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.message)
    }

    /**
     * Tests the SecondaryDeserializer by importing empty secondary deserializer - negative case.
     */
    @Test
    fun testSecondaryDeserializer_emptyDeserializer_throwsException() {
        val configs = getConfigsWithSecondaryDeserializer(EMPTY_SECONDARY_DESERIALIZER)
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                glueSchemaRegistryKafkaDeserializer.configure(configs, true)
            }

        assertEquals(EMPTY_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.message)
    }

    /**
     * Tests the SecondaryDeserializer by importing null secondary deserializer - negative case.
     */
    @Test
    fun testSecondaryDeserializer_nullDeserializer_throwsException() {
        val configs = getConfigsWithSecondaryDeserializer(null)
        val glueSchemaRegistryKafkaDeserializer = GlueSchemaRegistryKafkaDeserializer()

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                glueSchemaRegistryKafkaDeserializer.configure(configs, true)
            }

        assertEquals(NULL_SECONDARY_DESERIALIZER_EXCEPTION_MSG, exception.message)
    }

    private fun getConfigsWithSecondaryDeserializer(className: String?): Map<String, Any?> {
        val configs = HashMap<String, Any?>()
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER] = className
        return configs
    }

    private fun deserialize(
        deserializerConfigs: Map<String, Any?>,
        serializedBytes: ByteArray,
    ): Any? {
        val glueSchemaRegistryKafkaDeserializer =
            GlueSchemaRegistryKafkaDeserializer(this.mockCredProvider, deserializerConfigs)
        glueSchemaRegistryKafkaDeserializer.glueSchemaRegistryDeserializationFacade =
            mockGlueSchemaRegistryDeserializationFacade
        return glueSchemaRegistryKafkaDeserializer.deserialize(TEST_TOPIC, serializedBytes)
    }

    companion object {
        private const val VALID_SECONDARY_DESERIALIZER =
            "com.amazonaws.services.schemaregistry.deserializers.external.ThirdPartyDeserializer"
        private const val NON_KAFKA_SECONDARY_DESERIALIZER =
            "com.amazonaws.services.schemaregistry.deserializers.external.NotKafkaDeserializer"
        private const val EMPTY_SECONDARY_DESERIALIZER = ""
        private const val NON_KAFKA_SECONDARY_DESERIALIZER_EXCEPTION_MSG =
            "The secondary deserializer is not from Kafka"
        private const val EMPTY_SECONDARY_DESERIALIZER_EXCEPTION_MSG = "Can't find the class or instantiate it."
        private const val NULL_SECONDARY_DESERIALIZER_EXCEPTION_MSG =
            "Invalid secondary de-serializer configuration"
        private const val TEST_TOPIC = "TestTopic"
    }
}
