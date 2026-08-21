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
package com.amazonaws.services.schemaregistry.integrationtests.generators

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

class JsonSchemaGenericBackwardCompatDataGenerator : TestDataGenerator<JsonDataWithSchema> {
    /**
     * Method to generate Generic JSON Records
     *
     * @return List<JsonDataWithSchema>
     */
    override fun createRecords(): List<JsonDataWithSchema> {
        val genericJsonRecords = ArrayList<JsonDataWithSchema>()
        for (i in 1..2) {
            val jsonSchemaTestData = loadJson(String.format(JSON_TEST_DATA_FILE_PATH, i))
            val schemaNode = jsonSchemaTestData.get(SCHEMA_FIELD_NAME)
            for (dataNode in jsonSchemaTestData.get(PAYLOAD_FIELD_NAME)) {
                genericJsonRecords.add(
                    JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString()).build(),
                )
            }
        }
        return genericJsonRecords
    }

    companion object {
        private const val JSON_TEST_DATA_FILE_PATH = "src/test/resources/json/backward/backward%d.json"
        private const val SCHEMA_FIELD_NAME = "schema"
        private const val PAYLOAD_FIELD_NAME = "payload"

        @JvmStatic
        fun filterRecords(jsonDataWithSchema: JsonDataWithSchema): Boolean {
            val payload = jsonDataWithSchema.payload
            val jsonNode = ObjectMapper().readTree(payload)
            val f1: JsonNode? = jsonNode.get("f1")
            val f2: JsonNode? = jsonNode.get("f2")

            return !f1.toString().contains("Stranger") || f2.toString().toInt() != 911
        }
    }
}
