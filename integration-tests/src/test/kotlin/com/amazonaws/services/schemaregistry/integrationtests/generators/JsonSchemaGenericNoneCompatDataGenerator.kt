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

/**
 * Utility functions class for the generating the Generic Records for JSON Schema
 */
class JsonSchemaGenericNoneCompatDataGenerator : TestDataGenerator<JsonDataWithSchema> {
    /**
     * Method to generate Generic JSON Records
     *
     * @return List<JsonDataWithSchema>
     */
    override fun createRecords(): List<JsonDataWithSchema> {
        val genericJsonRecords = ArrayList<JsonDataWithSchema>()
        val jsonSchemaTestData = loadJson(JSON_TEST_DATA_FILE_PATH)
        for (objNode in jsonSchemaTestData) {
            if (objNode.get(VALID_FIELD_NAME).asBoolean()) {
                val testNode = objNode.get(TEST_FIELD_NAME)
                genericJsonRecords.add(
                    JsonDataWithSchema
                        .builder(
                            testNode.get(SCHEMA_FIELD_NAME).toString(),
                            testNode.get(PAYLOAD_FIELD_NAME).toString(),
                        ).build(),
                )
            }
        }
        return genericJsonRecords
    }

    companion object {
        private const val JSON_TEST_DATA_FILE_PATH = "src/test/resources/json/jsonSchemaTests.json"
        private const val SCHEMA_FIELD_NAME = "schema"
        private const val PAYLOAD_FIELD_NAME = "payload"
        private const val TEST_FIELD_NAME = "test"
        private const val VALID_FIELD_NAME = "valid"
    }
}
