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

/**
 * Avro Generic Record generator with None compatibility
 */
class AvroGenericNoneCompatDataGenerator : TestDataGenerator<GenericRecord> {
    override fun createRecords(): List<GenericRecord> {
        val parser = Schema.Parser()

        val schemaUser = parser.parse(File("src/test/resources/avro/user.avsc"))
        val schemaPayment = parser.parse(File("src/test/resources/avro/Payment.avsc"))

        val sansa: GenericRecord = GenericData.Record(schemaUser)
        sansa.put("name", "Sansa")
        sansa.put("favorite_number", 99)
        sansa.put("favorite_color", "white")

        val harry: GenericRecord = GenericData.Record(schemaUser)
        harry.put("name", "Harry")
        harry.put("favorite_number", 10)
        harry.put("favorite_color", "black")

        val hermione: GenericRecord = GenericData.Record(schemaUser)
        hermione.put("name", "Hermione")
        hermione.put("favorite_number", 1)
        hermione.put("favorite_color", "red")

        val ron: GenericRecord = GenericData.Record(schemaUser)
        ron.put("name", "Ron")
        ron.put("favorite_number", 18)
        ron.put("favorite_color", "green")

        val jay: GenericRecord = GenericData.Record(schemaUser)
        jay.put("name", "Jay")
        jay.put("favorite_number", 0)
        jay.put("favorite_color", "pink")

        val grocery: GenericRecord = GenericData.Record(schemaPayment)
        grocery.put("id", "grocery_1")
        grocery.put("amount", 25.5)

        val commute: GenericRecord = GenericData.Record(schemaPayment)
        commute.put("id", "commute_1")
        commute.put("amount", 3.5)

        val movie: GenericRecord = GenericData.Record(schemaPayment)
        movie.put("id", "entertainment_1")
        movie.put("amount", 19.2)

        val musical: GenericRecord = GenericData.Record(schemaPayment)
        musical.put("id", "entertainment_2")
        musical.put("amount", 105.0)

        val parking: GenericRecord = GenericData.Record(schemaPayment)
        parking.put("id", "commute_2")
        parking.put("amount", 15.0)

        return listOf(sansa, harry, hermione, ron, jay, grocery, commute, movie, musical, parking)
    }
}
