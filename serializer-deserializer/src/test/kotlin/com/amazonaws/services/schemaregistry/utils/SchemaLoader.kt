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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.avro.Schema
import org.junit.jupiter.api.Assertions.fail
import java.io.File

object SchemaLoader {
    private val JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true)
    private val OBJECT_MAPPER = ObjectMapper().setNodeFactory(JSON_NODE_FACTORY)

    /**
     * Loads and Parses schema from file.
     *
     * @param schemaFilePath Schema string
     * @return Avro schema object
     */
    @JvmStatic
    fun loadAvroSchema(schemaFilePath: String): Schema = try {
        Schema.Parser().parse(File(schemaFilePath))
    } catch (e: Exception) {
        fail("Failed to parse the avro schema file", e)
    }

    /**
     * Loads and Parses schema from file.
     *
     * @param jsonFilePath Schema string
     * @return JSON string
     */
    @JvmStatic
    fun loadJson(jsonFilePath: String): String = try {
        val file = File(jsonFilePath)
        OBJECT_MAPPER.readTree(file).toString()
    } catch (e: Exception) {
        fail("Failed to load the json file : ", e)
    }
}
