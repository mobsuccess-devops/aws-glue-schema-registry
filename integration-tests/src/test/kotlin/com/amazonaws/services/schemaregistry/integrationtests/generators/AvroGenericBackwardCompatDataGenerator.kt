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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File
import java.io.IOException

/**
 * Avro Generic Record generator with Backward compatibility
 */
class AvroGenericBackwardCompatDataGenerator : TestDataGenerator<GenericRecord> {
    override fun createRecords(): List<GenericRecord> {
        val parser = Schema.Parser()
        var schema1: Schema? = null
        var schema2: Schema? = null
        try {
            schema1 = parser.parse(File("src/test/resources/avro/backward/backward1.avsc"))
            schema2 = parser.parse(File("src/test/resources/avro/backward/backward2.avsc"))
        } catch (e: IOException) {
            e.printStackTrace()
        }

        val backward11: GenericRecord = GenericData.Record(schema1)
        backward11.put("id", "11")
        backward11.put("f1", "curfew")

        val backward12: GenericRecord = GenericData.Record(schema1)
        backward12.put("id", "12")
        backward12.put("f1", "covid-19")

        val backward21: GenericRecord = GenericData.Record(schema2)
        backward21.put("id", "21")
        backward21.put("f1", "happy")
        backward21.put("f2", "birthday")

        val backward22: GenericRecord = GenericData.Record(schema2)
        backward22.put("id", "22")
        backward22.put("f1", "merry")
        backward22.put("f2", "christmas")

        val backward13: GenericRecord = GenericData.Record(schema1)
        backward13.put("id", "13")
        backward13.put("f1", "social distancing")

        val backward23: GenericRecord = GenericData.Record(schema2)
        backward23.put("id", "23")
        backward23.put("f1", "good")
        backward23.put("f2", "morning")

        return listOf(backward11, backward12, backward21, backward22, backward13, backward23)
    }

    companion object {
        @JvmStatic
        fun filterRecords(genericRecord: GenericRecord): Boolean = "11" != genericRecord.get("id") || "covid-19" != genericRecord.get("f1")
    }
}
