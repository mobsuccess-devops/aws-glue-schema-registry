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
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import kotlin.jvm.javaObjectType

/**
 * A `Serde<T>` over a `Serde<Any>`, checking that what the delegate reads really is a [T].
 *
 * A primitive [type] is read as its wrapper: a deserializer hands back an `Integer`, never an
 * `int`, so checking against `int.class` would reject every record.
 *
 * @param type the type every record of the topic is expected to deserialize to
 * @param delegate the serde doing the work
 */
public class TypedSerde<T : Any>(
    type: Class<T>,
    private val delegate: Serde<Any>,
) : Serde<T> {
    private val type: Class<T> = type.kotlin.javaObjectType

    override fun serializer(): Serializer<T> = TypedSerializer(delegate.serializer())

    override fun deserializer(): Deserializer<T> = TypedDeserializer(type, delegate.deserializer())

    override fun configure(
        configs: Map<String, *>,
        isKey: Boolean,
    ) {
        delegate.configure(configs, isKey)
    }

    override fun close() {
        delegate.close()
    }

    private class TypedSerializer<T : Any>(
        private val delegate: Serializer<Any>,
    ) : Serializer<T> {
        override fun configure(
            configs: Map<String, *>,
            isKey: Boolean,
        ) {
            delegate.configure(configs, isKey)
        }

        override fun serialize(
            topic: String?,
            data: T?,
        ): ByteArray? = delegate.serialize(topic, data)

        override fun serialize(
            topic: String?,
            headers: Headers?,
            data: T?,
        ): ByteArray? = delegate.serialize(topic, headers, data)

        override fun close() {
            delegate.close()
        }
    }

    private class TypedDeserializer<T : Any>(
        private val type: Class<T>,
        private val delegate: Deserializer<Any>,
    ) : Deserializer<T> {
        override fun configure(
            configs: Map<String, *>,
            isKey: Boolean,
        ) {
            delegate.configure(configs, isKey)
        }

        override fun deserialize(
            topic: String?,
            data: ByteArray?,
        ): T? = cast(topic, delegate.deserialize(topic, data))

        override fun deserialize(
            topic: String?,
            headers: Headers?,
            data: ByteArray?,
        ): T? = cast(topic, delegate.deserialize(topic, headers, data))

        override fun close() {
            delegate.close()
        }

        private fun cast(
            topic: String?,
            value: Any?,
        ): T? {
            if (value == null || type.isInstance(value)) {
                return type.cast(value)
            }
            throw SerializationException(
                "Record of topic $topic deserialized to a ${value.javaClass.name}, " +
                    "which is not the ${type.name} this serde was created for",
            )
        }
    }
}
