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

package com.amazonaws.services.schemaregistry.caching

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import org.apache.avro.generic.GenericData
import org.apache.commons.lang3.builder.EqualsBuilder
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.glue.model.DataFormat
import java.io.File

class SchemaTest {
    @Test
    fun test_equality_Positive() {
        val schemaToTest = USER3_SCHEMA

        val objToTest = Schema(schemaToTest, DataFormat.AVRO.name, "test-schema")
        val fileName = "src/test/java/resources/avro/user3.avsc"
        val schema = getSchema(fileName)

        val genericRecordWithAllTypes = buildRecord(schema)

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes)

        val obj = Schema(schemaDefinition, DataFormat.AVRO.name, "test-schema")

        assertTrue(EqualsBuilder.reflectionEquals(objToTest, obj))
    }

    @Test
    fun test_inequality_Positive_1() {
        val schemaToTest = USER3_SCHEMA.replace("\"type\":\"record\"", "\"type\":\"record1\"")

        val objToTest = Schema(schemaToTest, DataFormat.AVRO.name, "test-schema")
        val fileName = "src/test/java/resources/avro/user3.avsc"
        val schema = getSchema(fileName)

        val genericRecordWithAllTypes = buildRecord(schema)

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes)

        val obj = Schema(schemaDefinition, DataFormat.AVRO.name, "test-schema")

        assertFalse(EqualsBuilder.reflectionEquals(objToTest, obj))
    }

    @Test
    fun test_inequality_Positive_2() {
        val schemaToTest = USER3_SCHEMA

        val objToTest = Schema(schemaToTest, "PROTOBUFF", "test-schema")
        val fileName = "src/test/java/resources/avro/user3.avsc"
        val schema = getSchema(fileName)

        val genericRecordWithAllTypes = buildRecord(schema)

        val schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes)

        val obj = Schema(schemaDefinition, DataFormat.AVRO.name, "test-schema")

        assertFalse(EqualsBuilder.reflectionEquals(objToTest, obj))
    }

    private fun buildRecord(schema: org.apache.avro.Schema): GenericData.Record {
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

    private fun getSchema(fileName: String): org.apache.avro.Schema {
        val parser = org.apache.avro.Schema.Parser()

        return parser.parse(File(fileName))
    }

    companion object {
        private const val USER3_SCHEMA =
            """{"type":"record","name":"User3","namespace":""" +
                """"com.amazonaws.services.schemaregistry.serializers.avro","doc":""" +
                """"This schema is created for testing purpose","fields":[{"name":"name","type":"string"},""" +
                """{"name":"favorite_number","type":["null","int"]},{"name":"meta","type":{"type":"map",""" +
                """"values":"long"}},{"name":"listOfColours","type":{"type":"array","items":"int"}},""" +
                """{"name":"integerEnum","type":{"type":"enum","name":"integerEnums","symbols":""" +
                """["ONE","TWO","THREE","FOUR"]}}]}"""
    }
}
