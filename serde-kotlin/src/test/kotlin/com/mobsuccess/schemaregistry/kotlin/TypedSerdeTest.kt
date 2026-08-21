/*
 * Copyright 2026 Mobsuccess.
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

package com.mobsuccess.schemaregistry.kotlin

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets

class TypedSerdeTest {
    @Test
    fun testDeserialize_returnsTheValueWhenItIsOfTheExpectedType() {
        val serde: Serde<String> = FakeSerde("hello").typed()

        assertEquals("hello", serde.deserializer().deserialize(TOPIC, BYTES))
    }

    @Test
    fun testDeserialize_returnsTheValueThroughTheHeadersOverload() {
        val serde: Serde<String> = FakeSerde("hello").typed()

        assertEquals("hello", serde.deserializer().deserialize(TOPIC, RecordHeaders(), BYTES))
    }

    @Test
    fun testDeserialize_returnsNullForANullValue() {
        val serde: Serde<String> = FakeSerde(null).typed()

        assertNull(serde.deserializer().deserialize(TOPIC, BYTES))
    }

    @Test
    fun testDeserialize_namesBothTypesWhenTheRecordIsOfAnother() {
        val serde: Serde<String> = FakeSerde(42).typed()

        val exception =
            assertThrows(SerializationException::class.java) {
                serde.deserializer().deserialize(TOPIC, BYTES)
            }

        assertTrue(exception.message!!.contains("java.lang.Integer"), exception.message)
        assertTrue(exception.message!!.contains("java.lang.String"), exception.message)
        assertTrue(exception.message!!.contains(TOPIC), exception.message)
    }

    @Test
    fun testDeserialize_acceptsASubtypeOfTheExpectedType() {
        val serde: Serde<Number> = FakeSerde(42).typed()

        assertEquals(42, serde.deserializer().deserialize(TOPIC, BYTES))
    }

    @Test
    fun testDeserialize_readsAPrimitiveTypeAsItsWrapper() {
        val serde: Serde<Int> = FakeSerde(42).typed()

        assertEquals(42, serde.deserializer().deserialize(TOPIC, BYTES))
    }

    @Test
    fun testDeserialize_readsAPrimitiveClassAsItsWrapper() {
        val serde = TypedSerde(Int::class.java, FakeSerde(42))

        assertEquals(42, serde.deserializer().deserialize(TOPIC, BYTES))
    }

    @Test
    fun testDeserialize_stillRejectsAnotherTypeForAPrimitiveClass() {
        val serde = TypedSerde(Int::class.java, FakeSerde("42"))

        assertThrows(SerializationException::class.java) {
            serde.deserializer().deserialize(TOPIC, BYTES)
        }
    }

    @Test
    fun testSerialize_passesTheValueThrough() {
        val delegate = FakeSerde("hello")
        val serde: Serde<String> = delegate.typed()

        assertArrayEquals(BYTES, serde.serializer().serialize(TOPIC, "hello"))
        assertArrayEquals(BYTES, serde.serializer().serialize(TOPIC, RecordHeaders(), "hello"))
        assertEquals(listOf("hello", "hello"), delegate.serialized)
    }

    @Test
    fun testConfigureAndClose_reachTheDelegate() {
        val delegate = FakeSerde("hello")
        val serde: Serde<String> = delegate.typed()

        serde.configure(mapOf("a" to "b"), true)
        serde.close()

        assertEquals(mapOf("a" to "b"), delegate.configuredWith)
        assertEquals(true, delegate.configuredAsKey)
        assertTrue(delegate.closed)
    }

    private class FakeSerde(
        private val value: Any?,
    ) : Serde<Any> {
        val serialized: MutableList<Any?> = mutableListOf()
        var configuredWith: Map<String, *>? = null
        var configuredAsKey: Boolean? = null
        var closed: Boolean = false

        override fun serializer(): Serializer<Any> = object : Serializer<Any> {
            override fun serialize(
                topic: String?,
                data: Any?,
            ): ByteArray {
                serialized.add(data)
                return BYTES
            }
        }

        override fun deserializer(): Deserializer<Any> = object : Deserializer<Any> {
            override fun deserialize(
                topic: String?,
                data: ByteArray?,
            ): Any? = value

            override fun deserialize(
                topic: String?,
                headers: Headers?,
                data: ByteArray?,
            ): Any? = value
        }

        override fun configure(
            configs: Map<String, *>,
            isKey: Boolean,
        ) {
            configuredWith = configs
            configuredAsKey = isKey
        }

        override fun close() {
            closed = true
        }
    }

    private companion object {
        private const val TOPIC = "User-Topic"
        private val BYTES = "hello".toByteArray(StandardCharsets.UTF_8)
    }
}
