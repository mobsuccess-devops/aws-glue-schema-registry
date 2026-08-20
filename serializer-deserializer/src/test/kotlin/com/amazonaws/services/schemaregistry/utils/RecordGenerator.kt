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

import com.amazonaws.services.schemaregistry.serializers.avro.User
import com.amazonaws.services.schemaregistry.serializers.json.Car
import com.amazonaws.services.schemaregistry.serializers.json.Employee
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.time.Instant
import java.util.Calendar
import java.util.Date
import java.util.GregorianCalendar
import java.util.TimeZone

object RecordGenerator {
    const val AVRO_USER_SCHEMA_FILE_PATH = "src/test/resources/avro/user.avsc"
    const val AVRO_EMP_RECORD_SCHEMA_FILE_PATH = "src/test/resources/avro/emp_record.avsc"
    const val AVRO_USER_ENUM_SCHEMA_FILE = "src/test/resources/avro/user_enum.avsc"
    const val AVRO_USER_ARRAY_SCHEMA_FILE = "src/test/resources/avro/user_array.avsc"
    const val AVRO_USER_UNION_SCHEMA_FILE = "src/test/resources/avro/user_union.avsc"
    const val AVRO_USER_FIXED_SCHEMA_FILE = "src/test/resources/avro/user_fixed.avsc"
    const val AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc"
    const val AVRO_USER_MAP_SCHEMA_FILE = "src/test/resources/avro/user_map.avsc"
    const val AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/resources/avro/user3.avsc"
    const val JSON_PERSON_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/person.schema.json"
    const val JSON_PERSON_DATA_FILE_PATH = "src/test/resources/json/person1.json"
    const val JSON_PRODUCE_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/produce_ref.schema.json"
    const val JSON_PRODUCE_DATA_FILE_PATH = "src/test/resources/json/produce1.json"
    const val JSON_GEOLOCATION_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/geographical-location.schema.json"
    const val JSON_GEOLOCATION_DATA_FILE_PATH = "src/test/resources/json/geolocation1.json"
    const val JSON_NULL_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/null.schema.json"
    const val JSON_NULL_DATA_FILE_PATH = "src/test/resources/json/null.json"
    const val JSON_STRING_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/string.schema.json"
    const val JSON_STRING_DATA_FILE_PATH = "src/test/resources/json/string.json"
    const val JSON_EMPTY_STRING_DATA_FILE_PATH = "src/test/resources/json/empty.json"
    const val JSON_NUMBER_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/number.schema.json"
    const val JSON_INTEGER_DATA_FILE_PATH = "src/test/resources/json/integer.json"
    const val JSON_FLOAT_DATA_FILE_PATH = "src/test/resources/json/float.json"
    const val JSON_BIG_INTEGER_DATA_FILE_PATH = "src/test/resources/json/bigint.json"
    const val JSON_BIG_DECIMAL_DATA_FILE_PATH = "src/test/resources/json/bigdecimal.json"
    const val JSON_INVALID_PRODUCE_1_DATA_FILE_PATH = "src/test/resources/json/invalidProduce1.json"
    const val JSON_INVALID_PRODUCE_2_DATA_FILE_PATH = "src/test/resources/json/invalidProduce2.json"
    const val JSON_DATE_TIME_ARRAY_DATA_FILE_PATH = "src/test/resources/json/dateTimeArray.json"
    const val JSON_DATE_TIME_ARRAY_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/dateTimeArray.schema.json"
    const val JSON_PRODUCT_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/product.schema.json"
    const val JSON_PRODUCT_URL_REF_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/productURLRef.schema.json"
    const val JSON_PRODUCT_INVALID_URL_REF_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/productInvalidURLRef.schema.json"
    const val JSON_PRODUCT_DATA_FILE_PATH = "src/test/resources/json/product.json"
    const val JSON_PERSON_RECURSIVE_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/personRecursive.schema.json"
    const val JSON_PERSON_RECURSIVE_DATA_FILE_PATH = "src/test/resources/json/personRecursive.json"
    const val JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft06/addressExtended.schema.json"
    const val JSON_ADDRESS1_DATA_FILE_PATH = "src/test/resources/json/address1.json"
    const val JSON_ADDRESS2_DATA_FILE_PATH = "src/test/resources/json/address2.json"
    const val JSON_ADDRESS_REF_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/addressRef.schema.json"
    const val JSON_ADDRESS_ID_REF_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/addressIdRef.schema.json"
    const val JSON_ADDRESS3_DATA_FILE_PATH = "src/test/resources/json/address3.json"
    const val JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/addressIfThenElse.schema.json"
    const val JSON_ADDRESS_USA_DATA_FILE_PATH = "src/test/resources/json/addressUSA.json"
    const val JSON_ADDRESS_CA_DATA_FILE_PATH = "src/test/resources/json/addressCA.json"
    const val JSON_ADDRESS_CA_INVALID_DATA_FILE_PATH = "src/test/resources/json/addressCAInvalid.json"
    const val JSON_ADDRESS_HTML_ENCODING_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/contentEncodingHtml.schema.json"
    const val JSON_ADDRESS_HTML_ENCODING_DATA_FILE_PATH = "src/test/resources/json/html.json"
    const val JSON_ADDRESS_BASE64_ENCODING_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft07/contentEncodingBase64Image.schema.json"
    const val JSON_ADDRESS_BASE64_ENCODING_DATA_FILE_PATH = "src/test/resources/json/base64EncodedImage.json"
    const val JSON_CONSTANT_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft06/constant.schema.json"
    const val JSON_CONSTANT_DATA_FILE_PATH = "src/test/resources/json/constant.json"
    const val JSON_CONSTANT_INVALID_DATA_FILE_PATH = "src/test/resources/json/constantInvalid.json"
    const val JSON_EMPLOYEE_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft04/employee.schema.json"
    const val JSON_EMPLOYEE_DATA_FILE_PATH = "src/test/resources/json/employee.json"
    const val JSON_WITHOUT_SPEC_SCHEMA_FILE_PATH =
        "src/test/resources/json/schema/draft04/withoutSchemaSpec.schema.json"
    const val JSON_NULL_SPEC_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft04/withoutSchemaSpec.schema.json"
    const val JSON_DATES_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/dates.schema.json"
    const val JSON_DATES_FILE_PATH = "src/test/resources/json/dates.json"
    const val JSON_BOOLEAN_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/boolean.schema.json"
    const val JSON_BOOLEAN_FILE_PATH = "src/test/resources/json/boolean.json"
    const val JSON_DDB_SCHEMA_FILE_PATH = "src/test/resources/json/schema/draft07/ddb.schema.json"
    const val JSON_DDB_FILE_PATH = "src/test/resources/json/ddb.json"

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericAvroRecord(): GenericRecord {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE_PATH)
        val genericRecord: GenericRecord = GenericData.Record(schema)
        genericRecord.put("name", "sansa")
        genericRecord.put("favorite_number", 99)
        genericRecord.put("favorite_color", "red")

        return genericRecord
    }

    /**
     * Test Helper method to generate a test ENUM GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUserEnumAvroRecord(): GenericData.EnumSymbol {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE)
        return GenericData.EnumSymbol(schema, "ONE")
    }

    /**
     * Test Helper method to generate an invalid test Avro ENUM GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUserInvalidEnumAvroRecord(): GenericData.EnumSymbol {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE)
        return GenericData.EnumSymbol(schema, "SPADE")
    }

    /**
     * Test Helper method to generate a test Avro integer Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericIntArrayAvroRecord(): GenericData.Array<Int> {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE)
        val array = GenericData.Array<Int>(1, schema)
        array.add(1)

        return array
    }

    /**
     * Test Helper method to generate a test Avro String Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericStringArrayAvroRecord(): GenericData.Array<String> {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE)
        val array = GenericData.Array<String>(1, schema)
        array.add("2")

        return array
    }

    /**
     * Test Helper method to generate a test Avro invalid Array GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUserInvalidArrayAvroRecord(): GenericData.Array<Any> {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE)
        val array = GenericData.Array<Any>(1, schema)
        array.add("s")

        return array
    }

    /**
     * Test Helper method to generate a test Avro Map GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUserMapAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE)
        val mapRecord = GenericData.Record(schema)
        val map = HashMap<String, Long>()
        map["test"] = 1L
        mapRecord.put("meta", map)

        return mapRecord
    }

    /**
     * Test Helper method to generate a test invalid Avro Map GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericInvalidMapAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE)
        val mapRecord = GenericData.Record(schema)
        val map = HashMap<String, Any>()
        map["test"] = "s"
        mapRecord.put("meta", map)

        return mapRecord
    }

    /**
     * Test Helper method to generate a test Avro Union GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUserUnionAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE)
        val unionRecord = GenericData.Record(schema)
        unionRecord.put("experience", 1)
        unionRecord.put("age", 30)

        return unionRecord
    }

    /**
     * Test Helper method to generate a test Avro Union GenericRecord with null
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericUnionWithNullValueAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE)
        val unionRecord = GenericData.Record(schema)
        unionRecord.put("experience", null)
        unionRecord.put("age", 30)

        return unionRecord
    }

    /**
     * Test Helper method to generate a test Avro invalid Union GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericInvalidUnionAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE)
        val unionRecord = GenericData.Record(schema)
        unionRecord.put("experience", "wrong_value")
        unionRecord.put("age", 30)

        return unionRecord
    }

    /**
     * Test Helper method to generate a test Avro Fixed GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericFixedAvroRecord(): GenericData.Fixed {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE)
        val fixedRecord = GenericData.Fixed(schema)
        val bytes = "byte array".toByteArray()
        fixedRecord.bytes(bytes)

        return fixedRecord
    }

    /**
     * Test Helper method to generate a test Avro Invalid Fixed GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericInvalidFixedAvroRecord(): GenericData.Fixed {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE)
        val fixedRecord = GenericData.Fixed(schema)
        val bytes = "byte".toByteArray()
        fixedRecord.bytes(bytes)

        return fixedRecord
    }

    /**
     * Test Helper method to generate a test Avro mized types GenericRecord
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericMultipleTypesAvroRecord(): GenericData.Record {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE)

        val k = GenericData.EnumSymbol(schema, "ONE")
        val al = ArrayList<Int>()
        al.add(1)

        val genericRecordWithAllTypes = GenericData.Record(schema)
        val map = HashMap<String, Long>()
        map["test"] = 1L

        genericRecordWithAllTypes.put("name", "Joe")
        genericRecordWithAllTypes.put("favorite_number", 1)
        genericRecordWithAllTypes.put("meta", map)
        genericRecordWithAllTypes.put("listOfColours", al)
        genericRecordWithAllTypes.put("integerEnum", k)

        return genericRecordWithAllTypes
    }

    /**
     * Helper method to create a test user object
     *
     * @return constructed user object instance
     */
    @JvmStatic
    fun createSpecificAvroRecord(): User = User
        .newBuilder()
        .setName("test")
        .setFavoriteColor("violet")
        .setFavoriteNumber(10)
        .build()

    /**
     * Helper method to generate a test GenericRecord from emp record schema
     *
     * @return Generic AVRO Record
     */
    @JvmStatic
    fun createGenericEmpRecord(): GenericRecord {
        val schema = SchemaLoader.loadAvroSchema(AVRO_EMP_RECORD_SCHEMA_FILE_PATH)
        val genericRecord: GenericRecord = GenericData.Record(schema)
        genericRecord.put("name", "xyz")
        genericRecord.put("id", 1)
        genericRecord.put("salary", 30000)
        genericRecord.put("age", 25)
        genericRecord.put("address", "abc")
        return genericRecord
    }

    /**
     * Test Helper method to generate a test GenericRecord
     *
     * @return JsonDataWithSchema
     */
    @JvmStatic
    fun createGenericJsonRecord(testJsonRecord: TestJsonRecord): JsonDataWithSchema {
        val schema = SchemaLoader.loadJson(testJsonRecord.schemaPath)
        val payload = SchemaLoader.loadJson(testJsonRecord.dataPath)

        return JsonDataWithSchema.builder(schema, payload).build()
    }

    /**
     * Test Helper method to generate a GenericRecord with invalid schema
     *
     * @return JsonDataWithSchema
     */
    @JvmStatic
    fun createRecordWithMalformedJsonSchema(): JsonDataWithSchema {
        val schema =
            """
            {
              "${'$'}id": "https://example.com/string.schema.json",
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "description": "String schema",
              "type": "string",
              "additionalProperties": false,
            }
            """.trimIndent()
        val payload = "abcd"

        return JsonDataWithSchema.builder(schema, payload).build()
    }

    /**
     * Test Helper method to generate a GenericRecord with invalid schema
     *
     * @return JsonDataWithSchema
     */
    @JvmStatic
    fun createRecordWithMalformedJsonData(): JsonDataWithSchema {
        val schema =
            """
            {
              "${'$'}id": "https://example.com/geographical-location.schema.json",
              "${'$'}schema": "http://json-schema.org/draft-07/schema#",
              "title": "Longitude and Latitude Values",
              "description": "A geographical coordinate.",
              "required": [ "latitude", "longitude" ],
              "type": "object",
              "properties": {
                "latitude": {
                  "type": "number",
                  "minimum": -90,
                  "maximum": 90
                },
                "longitude": {
                  "type": "number",
                  "minimum": -180,
                  "maximum": 180
                }
              },
              "additionalProperties": false
            }
            """.trimIndent()
        val payload =
            """
            {
              "latitude": 48.858093,
              "longitude": 2.294694,
            }
            """.trimIndent()

        return JsonDataWithSchema.builder(schema, payload).build()
    }

    @JvmStatic
    fun createNonSchemaConformantJsonData(): JsonDataWithSchema {
        val invalidTestRecord = TestJsonRecord.valueOf(TestJsonRecord.INVALIDPERSON.name)
        return createGenericJsonRecord(invalidTestRecord)
    }

    /**
     * Helper method to create a test specific record of type Car
     *
     * @return constructed user object instance
     */
    @JvmStatic
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
            .serviceChecks(listOf(5000.0f, 10780.30f))
            .build()
    }

    /**
     * Helper method to create a test invalid specific record of type Car
     *
     * @return constructed user object instance
     */
    @JvmStatic
    fun createInvalidSpecificJsonRecord(): Car = Car
        .builder()
        .make("Honda")
        .model("crv")
        .used(true)
        .miles(300000)
        .year(1999)
        .owners(arrayOf("John", "Jane", "Hu"))
        .serviceChecks(listOf(5000.0f, 10780.30f))
        .build()

    /**
     * Helper method to create a test invalid specific record of type Employee
     * with wrong class name
     *
     * @return constructed user object instance
     */
    @JvmStatic
    fun createInvalidEmployeeJsonRecord(): Employee = Employee
        .builder()
        .name("JohnDoe")
        .build()

    /**
     * Helper method to create a test null specific record
     *
     * @return constructed user object instance
     */
    @JvmStatic
    fun createNullSpecificJsonRecord(): Any = Any()

    enum class TestJsonRecord(
        val schemaPath: String,
        val dataPath: String,
        val isValid: Boolean,
    ) {
        PERSON(JSON_PERSON_SCHEMA_FILE_PATH, JSON_PERSON_DATA_FILE_PATH, true),
        PRODUCE(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_PRODUCE_DATA_FILE_PATH, true),
        GEOLOCATION(JSON_GEOLOCATION_SCHEMA_FILE_PATH, JSON_GEOLOCATION_DATA_FILE_PATH, true),
        NULLSTRING(JSON_NULL_SCHEMA_FILE_PATH, JSON_NULL_DATA_FILE_PATH, true),
        NONEMPTYSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_STRING_DATA_FILE_PATH, true),
        EMPTYSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_EMPTY_STRING_DATA_FILE_PATH, true),
        INTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        FLOAT(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_FLOAT_DATA_FILE_PATH, true),
        BIGINTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_BIG_INTEGER_DATA_FILE_PATH, true),
        BIGDECIMAL(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_BIG_DECIMAL_DATA_FILE_PATH, true),
        DATETIMEARRAY(JSON_DATE_TIME_ARRAY_SCHEMA_FILE_PATH, JSON_DATE_TIME_ARRAY_DATA_FILE_PATH, true),
        PERSONRECURSIVE(JSON_PERSON_RECURSIVE_SCHEMA_FILE_PATH, JSON_PERSON_RECURSIVE_DATA_FILE_PATH, true),

        // true if local reference is allowed
        PRODUCT_REMOTE_REF(JSON_PRODUCT_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false),
        PRODUCT_INVALID_URL_REF(JSON_PRODUCT_INVALID_URL_REF_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false),

        // true if remote reference is allowed
        PRODUCT_URL_REF(JSON_PRODUCT_URL_REF_SCHEMA_FILE_PATH, JSON_PRODUCT_DATA_FILE_PATH, false),
        ADDRESSREF(JSON_ADDRESS_REF_SCHEMA_FILE_PATH, JSON_ADDRESS3_DATA_FILE_PATH, true),
        ADDRESSIDREF(JSON_ADDRESS_ID_REF_SCHEMA_FILE_PATH, JSON_ADDRESS3_DATA_FILE_PATH, true),
        ADDRESSEXTENDED(JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH, JSON_ADDRESS1_DATA_FILE_PATH, true),
        ADDRESSEXTENDEDINVALID(JSON_ADDRESS_EXTENDED_SCHEMA_FILE_PATH, JSON_ADDRESS2_DATA_FILE_PATH, false),
        ADDRESSUSA(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_USA_DATA_FILE_PATH, true),
        ADDRESSCA(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_CA_DATA_FILE_PATH, true),
        ADDRESSCAINVALID(JSON_ADDRESS_IF_ELSE_SCHEMA_FILE_PATH, JSON_ADDRESS_CA_INVALID_DATA_FILE_PATH, false),
        HTMLENCODED(JSON_ADDRESS_HTML_ENCODING_SCHEMA_FILE_PATH, JSON_ADDRESS_HTML_ENCODING_DATA_FILE_PATH, true),
        BASE64ENCODED(JSON_ADDRESS_BASE64_ENCODING_SCHEMA_FILE_PATH, JSON_ADDRESS_BASE64_ENCODING_DATA_FILE_PATH, true),
        CONSTANT(JSON_CONSTANT_SCHEMA_FILE_PATH, JSON_CONSTANT_DATA_FILE_PATH, true),
        CONSTANTINVALID(JSON_CONSTANT_SCHEMA_FILE_PATH, JSON_CONSTANT_INVALID_DATA_FILE_PATH, false),
        INVALIDPERSON(JSON_PERSON_SCHEMA_FILE_PATH, JSON_PRODUCE_DATA_FILE_PATH, false),
        INVALIDPRODUCE1(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_INVALID_PRODUCE_1_DATA_FILE_PATH, false),
        INVALIDPRODUCE2(JSON_PRODUCE_SCHEMA_FILE_PATH, JSON_INVALID_PRODUCE_2_DATA_FILE_PATH, false),
        INVALIDSTRING(JSON_STRING_SCHEMA_FILE_PATH, JSON_NULL_DATA_FILE_PATH, false),
        INVALIDINTEGER(JSON_NUMBER_SCHEMA_FILE_PATH, JSON_STRING_DATA_FILE_PATH, false),
        EMPLOYEE(JSON_EMPLOYEE_SCHEMA_FILE_PATH, JSON_EMPLOYEE_DATA_FILE_PATH, true),
        WITHOUTSCHEMASPEC(JSON_WITHOUT_SPEC_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        NULLSCHEMASPEC(JSON_NULL_SPEC_SCHEMA_FILE_PATH, JSON_INTEGER_DATA_FILE_PATH, true),
        DATES(JSON_DATES_SCHEMA_FILE_PATH, JSON_DATES_FILE_PATH, true),
        BOOLEAN(JSON_BOOLEAN_SCHEMA_FILE_PATH, JSON_BOOLEAN_FILE_PATH, true),
        DDB(JSON_DDB_SCHEMA_FILE_PATH, JSON_DDB_FILE_PATH, true),
    }
}
