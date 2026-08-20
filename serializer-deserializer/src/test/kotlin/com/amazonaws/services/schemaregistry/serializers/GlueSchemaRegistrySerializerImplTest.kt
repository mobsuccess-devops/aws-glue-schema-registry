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

package com.amazonaws.services.schemaregistry.serializers

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistrySerializerImplTest {
    private lateinit var glueSchemaRegistrySerializer: GlueSchemaRegistrySerializerImpl

    @Mock
    private lateinit var credentialsProvider: AwsCredentialsProvider

    @Mock
    private lateinit var glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade

    @BeforeEach
    fun setUp() {
        glueSchemaRegistrySerializer = GlueSchemaRegistrySerializerImpl(glueSchemaRegistrySerializationFacade)
    }

    @Test
    fun instantiate_WithNullConfiguration_CreatesInstance() {
        val glueSchemaRegistrySerializer: GlueSchemaRegistrySerializer =
            GlueSchemaRegistrySerializerImpl(credentialsProvider, GlueSchemaRegistryConfiguration(REGION))

        assertNotNull(glueSchemaRegistrySerializer)
    }

    @Test
    fun instantiate_WithConfiguration_CreatesInstance() {
        val configuration = GlueSchemaRegistryConfiguration(REGION)

        val glueSchemaRegistrySerializer: GlueSchemaRegistrySerializer =
            GlueSchemaRegistrySerializerImpl(credentialsProvider, configuration)

        assertNotNull(glueSchemaRegistrySerializer)
    }

    @Test
    fun getSchema_WhenASchemaIsPassed_EncodesIntoSchemaRegistryMessage() {
        doReturn(ENCODED_DATA)
            .whenever(glueSchemaRegistrySerializationFacade)
            .encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA)

        val actual = glueSchemaRegistrySerializer.encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA)

        assertEquals(ENCODED_DATA, actual)
    }

    companion object {
        private const val TRANSPORT_NAME = "stream-foo"
        private const val REGION = "us-west-2"

        private val ENCODED_DATA = byteArrayOf(8, 9, 12, 83, 82)
        private val USER_DATA = byteArrayOf(12, 83, 82)
        private val SCHEMA_REGISTRY_SCHEMA = Schema("{}", "AVRO", "schemaFoo")
    }
}
