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

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.avro.Schema
import org.hamcrest.CoreMatchers.instanceOf
import org.hamcrest.CoreMatchers.notNullValue
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File

class GlueSchemaRegistryAvroDeserializationSchemaTest {
    @BeforeEach
    fun setup() {
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING] = true

        userSchema = Schema.Parser().parse(File(AVRO_USER_SCHEMA_FILE))
    }

    /**
     * Test whether forGeneric method works
     */
    @Test
    fun testForGeneric_withValidParams_succeeds() {
        assertThat(GlueSchemaRegistryAvroDeserializationSchema.forGeneric(userSchema, configs), notNullValue())
        assertThat(
            GlueSchemaRegistryAvroDeserializationSchema.forGeneric(userSchema, configs),
            instanceOf(GlueSchemaRegistryAvroDeserializationSchema::class.java),
        )
    }

    /**
     * Test whether forSpecific method works
     */
    @Test
    fun testForSpecific_withValidParams_succeeds() {
        assertThat(GlueSchemaRegistryAvroDeserializationSchema.forSpecific(User::class.java, configs), notNullValue())
        assertThat(
            GlueSchemaRegistryAvroDeserializationSchema.forSpecific(User::class.java, configs),
            instanceOf(GlueSchemaRegistryAvroDeserializationSchema::class.java),
        )
    }

    companion object {
        private lateinit var userSchema: Schema
        private val configs: MutableMap<String, Any> = HashMap()

        private const val AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc"
    }
}
