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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

/**
 * Factory Test to create a new instance of TypeConverter.
 */
class TypeConverterFactoryTest {
    private val typeConverterFactory = TypeConverterFactory()

    /**
     * Test for Type Converter instance creation.
     */
    @ParameterizedTest
    @EnumSource(value = Schema.Type::class)
    fun testGetTypeConverter_createObject_succeeds(schemaType: Schema.Type) {
        val typeConverter = typeConverterFactory.get(schemaType)

        assertNotNull(typeConverter)
        assertTrue(
            typeConverter!!
                .javaClass
                .simpleName
                .lowercase()
                .startsWith(schemaType.name.lowercase()),
        )
    }

    /**
     * Test for type converter instance by logical name
     */
    @ParameterizedTest
    @MethodSource("testLogicalNameArgumentsProvider")
    fun testGetTypeConverter_ByLogicalName_succeeds(logicalName: String) {
        val typeConverter = typeConverterFactory.get(logicalName)
        assertNotNull(typeConverterFactory.get(logicalName))

        val logicalNameSplit = logicalName.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }

        assertTrue(
            typeConverter!!
                .javaClass
                .simpleName
                .startsWith(logicalNameSplit[logicalNameSplit.size - 1]),
        )
    }

    /**
     * Test for unknown type converter instance
     */
    @Test
    fun testGetTypeConverter_UnsupportedType_returnsNull() {
        assertNull(typeConverterFactory.get("INT128"))
    }

    companion object {
        @JvmStatic
        private fun testLogicalNameArgumentsProvider(): Stream<Arguments> = Stream.of(
            Arguments.of(Decimal.LOGICAL_NAME),
            Arguments.of(Date.LOGICAL_NAME),
            Arguments.of(Time.LOGICAL_NAME),
            Arguments.of(Timestamp.LOGICAL_NAME),
        )
    }
}
