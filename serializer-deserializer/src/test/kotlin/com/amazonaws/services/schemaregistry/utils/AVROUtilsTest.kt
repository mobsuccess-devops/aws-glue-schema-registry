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

package com.amazonaws.services.schemaregistry.utils

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.serializers.avro.User
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.io.File
import java.io.IOException

class AVROUtilsTest {
    private lateinit var mockClient: AWSSchemaRegistryClient
    private val configs: MutableMap<String, Any> = HashMap()
    private var schema: Schema? = null
    private lateinit var userDefinedPojo: User
    private lateinit var genericRecord: GenericRecord
    private lateinit var customer: Customer

    @BeforeEach
    fun setup() {
        mockClient = mock<AWSSchemaRegistryClient>()

        customer = Customer()
        customer.name = "test"

        val parser = Schema.Parser()
        try {
            schema = parser.parse(File("src/test/resources/avro/user.avsc"))
        } catch (e: IOException) {
            fail<Unit>("Catch IOException: ", e)
        }

        userDefinedPojo =
            User
                .newBuilder()
                .setName("test_avros_schema")
                .setFavoriteColor("violet")
                .setFavoriteNumber(10)
                .build()

        genericRecord = GenericData.Record(schema)
        genericRecord.put("name", "sansa")
        genericRecord.put("favorite_number", 99)
        genericRecord.put("favorite_color", "red")

        configs["endpoint"] = "https://mjguu1u07a.execute-api.us-west-2.amazonaws.com/beta"
        configs["region"] = "us-west-2"
        configs["compression"] = "NONE"
    }

    @Test
    fun testGetSchemaDefinition_pojo_schemaDefinitionMatches() {
        assertEquals(USER_SCHEMA, AVROUtils.getInstance().getSchemaDefinition(userDefinedPojo))
    }

    @Test
    fun testGetSchemaDefinition_parseSchema_schemaDefinitionMatches() {
        assertEquals(USER_SCHEMA, AVROUtils.getInstance().getSchemaDefinition(genericRecord))
    }

    @Test
    fun testGetSchemaDefinition_nonAVROSchema_throwsException() {
        Assertions.assertThrows(AWSSchemaRegistryException::class.java) {
            assertNull(AVROUtils.getInstance().getSchemaDefinition(customer))
        }
    }

    @Test
    fun testGetSchemaDefinition_nullObject_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            AVROUtils.getInstance().getSchemaDefinition(nullOf())
        }
    }

    @Test
    fun testGetSchema_nullObject_throwsException() {
        Assertions.assertThrows(NullPointerException::class.java) {
            AVROUtils.getInstance().getSchema(nullOf())
        }
    }

    companion object {
        private const val USER_SCHEMA =
            """{"type":"record","name":"User","namespace":""" +
                """"com.amazonaws.services.schemaregistry.serializers.avro","fields":[{"name":"name",""" +
                """"type":"string"},{"name":"favorite_number","type":["int","null"]},""" +
                """{"name":"favorite_color","type":["string","null"]}]}"""
    }
}

class Customer {
    var name: String? = null
}
