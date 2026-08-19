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

import com.amazonaws.services.schemaregistry.common.Schema

/**
 * Entry point to deserialization capabilities of Glue Schema Registry client library.
 */
interface GlueSchemaRegistryDeserializer {
    /**
     * Returns plain customer data from a Glue Schema Registry encoded Byte array.
     */
    fun getData(data: ByteArray): ByteArray

    /**
     * Returns the schema encoded in the byte array by Glue Schema Registry serializer.
     */
    fun getSchema(data: ByteArray): Schema

    /**
     * Determines if the given byte array can be deserialized by Glue Schema Registry deserializer.
     */
    fun canDeserialize(data: ByteArray?): Boolean

    /**
     * Overrides the UserAgentApp name attribute at runtime.
     */
    fun overrideUserAgentApp(name: String?)
}
