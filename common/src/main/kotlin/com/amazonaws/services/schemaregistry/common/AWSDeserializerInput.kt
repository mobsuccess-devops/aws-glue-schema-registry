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

package com.amazonaws.services.schemaregistry.common

import java.nio.ByteBuffer

/**
 * Encapsulates general inputs for deserializer
 */
class AWSDeserializerInput(
    val buffer: ByteBuffer,
    transportName: String?,
) {
    val transportName: String? = transportName ?: DEFAULT_TRANSPORT_NAME

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AWSDeserializerInput) return false
        return buffer == other.buffer && transportName == other.transportName
    }

    override fun hashCode(): Int = 31 * buffer.hashCode() + (transportName?.hashCode() ?: 0)

    override fun toString(): String = "AWSDeserializerInput(buffer=$buffer, transportName=$transportName)"

    /** Reprend l'API fluide que générait Lombok : appelée depuis du code Java. */
    class Builder internal constructor() {
        private var buffer: ByteBuffer? = null
        private var transportName: String? = null

        fun buffer(buffer: ByteBuffer?): Builder = apply { this.buffer = buffer }

        fun transportName(transportName: String?): Builder = apply { this.transportName = transportName }

        fun build(): AWSDeserializerInput = AWSDeserializerInput(requireNotNull(buffer) { "buffer is marked non-null but is null" }, transportName)
    }

    companion object {
        private const val DEFAULT_TRANSPORT_NAME = "default-stream"

        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
