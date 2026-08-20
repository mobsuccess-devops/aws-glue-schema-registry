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

package com.amazonaws.services.schemaregistry.kafkastreams.utils

import com.amazonaws.services.schemaregistry.kafkastreams.utils.avro.User
import com.amazonaws.services.schemaregistry.kafkastreams.utils.json.Car
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.fail
import java.io.File
import java.time.Instant
import java.util.Arrays
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.TimeZone

object RecordGenerator {
    const val AVRO_USER_SCHEMA_FILE_PATH = "src/test/resources/avro/user.avsc"
    const val JSON_PERSON_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/person.schema.json"
    const val JSON_PERSON_DATA_FILE_PATH = "src/test/resources/json/person1.json"

    private val objectMapper = ObjectMapper()

    /**
     * Loads and Parses schema from file.
     *
     * @param schemaFilePath Schema string
     * @return Avro schema object
     */
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
    fun loadJson(jsonFilePath: String): String = try {
        objectMapper.readTree(File(jsonFilePath)).toString()
    } catch (e: Exception) {
        fail("Failed to load the json file : $jsonFilePath", e)
    }

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return Generic AVRO Record
     */
    fun createGenericAvroRecord(): GenericRecord {
        val schema = loadAvroSchema(AVRO_USER_SCHEMA_FILE_PATH)
        val genericRecord: GenericRecord = GenericData.Record(schema)
        genericRecord.put("name", "sansa")
        genericRecord.put("favorite_number", 99)
        genericRecord.put("favorite_color", "red")

        return genericRecord
    }

    /**
     * Helper method to create a test user object
     *
     * @return constructed user object instance
     */
    fun createSpecificAvroRecord(): User = User
        .newBuilder()
        .setName("test")
        .setFavoriteColor("violet")
        .setFavoriteNumber(10)
        .build()

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return JsonDataWithSchema
     */
    fun createGenericJsonRecord(): JsonDataWithSchema {
        val schema = loadJson(JSON_PERSON_SCHEMA_FILE_PATH)
        val data = loadJson(JSON_PERSON_DATA_FILE_PATH)

        return JsonDataWithSchema.builder(schema, data).build()
    }

    /**
     * Helper method to create a test specific record of type Car
     *
     * @return constructed user object instance
     */
    fun createSpecificJsonRecord(): Car {
        val calendar = GregorianCalendar(2014, Calendar.FEBRUARY, 11)
        calendar.timeZone = TimeZone.getTimeZone("PST")

        return Car
            .builder()
            .make("Honda")
            .model("crv")
            .used(true)
            .miles(10000)
            .year(2016)
            .listedDate(calendar.time)
            .purchaseDate(Date.from(Instant.parse("2000-01-01T00:00:00.000Z")))
            .owners(arrayOf("John", "Jane", "Hu"))
            .serviceChecks(Arrays.asList(5000.0f, 10780.30f))
            .build()
    }
}
