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
 * Avro Generic Record generator with Backward All compatibility
 */
class AvroGenericBackwardAllCompatDataGenerator : TestDataGenerator<GenericRecord> {
    override fun createRecords(): List<GenericRecord> {
        val parser = Schema.Parser()
        var schema1: Schema? = null
        var schema2: Schema? = null
        var schema3: Schema? = null
        try {
            schema1 = parser.parse(File("src/test/resources/avro/backwardAll/backwardAll1.avsc"))
            schema2 = parser.parse(File("src/test/resources/backwardAll/backwardAll2.avsc"))
            schema3 = parser.parse(File("src/test/resources/backwardAll/backwardAll3.avsc"))
        } catch (e: IOException) {
            e.printStackTrace()
        }

        val backwardAll11: GenericRecord = GenericData.Record(schema1)
        backwardAll11.put("f1", "game")
        backwardAll11.put("f2", "station")
        backwardAll11.put("f3", 0)

        val backwardAll21: GenericRecord = GenericData.Record(schema2)
        backwardAll21.put("f1", "disney")
        backwardAll21.put("f2", "plus")

        val backwardAll31: GenericRecord = GenericData.Record(schema3)
        backwardAll31.put("f1", "ladies")

        val backwardAll12: GenericRecord = GenericData.Record(schema1)
        backwardAll12.put("f1", "tamper")
        backwardAll12.put("f2", "monkey")
        backwardAll12.put("f3", 1)

        val backwardAll22: GenericRecord = GenericData.Record(schema2)
        backwardAll22.put("f1", "hbo")
        backwardAll22.put("f2", "max")

        val backwardAll32: GenericRecord = GenericData.Record(schema3)
        backwardAll32.put("f1", "gentlemen")

        return listOf(backwardAll11, backwardAll21, backwardAll31, backwardAll12, backwardAll22, backwardAll32)
    }
}
