/*
 * Copyright 2019 Confluent Inc.
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
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

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import io.test.avro.core.AvroMessage
import io.test.avro.doc.DocTestRecord
import io.test.avro.union.FirstOption
import io.test.avro.union.MultiTypeUnionMessage
import io.test.avro.union.SecondOption
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.reflect.ReflectData
import org.apache.avro.reflect.Union
import org.apache.avro.specific.SpecificData
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import uk.co.jemos.podam.api.PodamFactoryImpl
import java.io.File

class AdditionalAvroDataTest {
    private lateinit var avroData: AvroData

    @Before
    fun before() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        avroData = AvroData(avroDataConfig)
    }

    @Test
    fun testDocumentationPreservedSchema() {
        val avroSchema = Parser().parse(File("src/test/avro/DocTestRecord.avsc"))

        val connectSchema = avroData.toConnectSchema(avroSchema)

        val outputAvroSchema = avroData.fromConnectSchema(connectSchema)

        Assert.assertEquals(avroSchema, outputAvroSchema)
    }

    @Test
    fun testDocumentationPreservedData() {
        val factory = PodamFactoryImpl()

        val testRecord = factory.manufacturePojo(DocTestRecord::class.java)
        val connectSchemaAndValue = avroData.toConnectData(testRecord.schema, testRecord)!!

        val output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())

        Assert.assertEquals(SpecificData.get().toString(testRecord), SpecificData.get().toString(output))
    }

    @Test
    fun testComplexUnionSchema() {
        // Here is a schema complex union schema
        val avroSchema = Parser().parse(File("src/test/avro/AvroMessage.avsc"))

        val connectSchema = avroData.toConnectSchema(avroSchema)

        val outputAvroSchema = avroData.fromConnectSchema(connectSchema)

        Assert.assertEquals(avroSchema, outputAvroSchema)
    }

    @Test
    fun testComplexUnionData() {
        val factory = PodamFactoryImpl()

        val avroMessage = factory.manufacturePojo(AvroMessage::class.java)
        val connectSchemaAndValue = avroData.toConnectData(avroMessage.schema, avroMessage)!!

        val output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())

        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output))
    }

    @Test
    fun testComplexMultiUnionData() {
        val factory = PodamFactoryImpl()

        val avroMessage = factory.manufacturePojo(MultiTypeUnionMessage::class.java)

        var connectSchemaAndValue = avroData.toConnectData(avroMessage.schema, avroMessage)!!
        var output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output))

        avroMessage.setCompositeRecord(FirstOption("x", 2L))
        connectSchemaAndValue = avroData.toConnectData(avroMessage.schema, avroMessage)!!
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output))

        avroMessage.setCompositeRecord(SecondOption("y", 3L))
        connectSchemaAndValue = avroData.toConnectData(avroMessage.schema, avroMessage)!!
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output))

        avroMessage.setCompositeRecord(listOf("1", "2"))
        connectSchemaAndValue = avroData.toConnectData(avroMessage.schema, avroMessage)!!
        output = avroData.fromConnectData(connectSchemaAndValue.schema(), connectSchemaAndValue.value())
        Assert.assertEquals(SpecificData.get().toString(avroMessage), SpecificData.get().toString(output))
    }

    @Test
    fun testNestedUnion() {
        // Cannot use AllowNull to generate schema
        // because Avro 1.7.7 will throw org.apache.avro.AvroRuntimeException: Nested union
        // Schema myAvroObjectSchema = AllowNull.get().getSchema(MyObjectToPersist.class);

        // Here is a schema generated by Avro 1.8.1
        val myAvroObjectSchema =
            Parser().parse(
                "{" +
                    "  \"type\" : \"record\"," +
                    "  \"name\" : \"MyObjectToPersist\"," +
                    "  \"namespace\" : " +
                    "\"com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AdditionalAvroDataTest\"," +
                    "  \"fields\" : [ {" +
                    "    \"name\" : \"obj\"," +
                    "    \"type\" : [ \"null\", {" +
                    "      \"type\" : \"record\"," +
                    "      \"name\" : \"MyImpl1\"," +
                    "      \"fields\" : [ {" +
                    "        \"name\" : \"data\"," +
                    "        \"type\" : [ \"null\", \"string\" ]," +
                    "        \"default\" : null" +
                    "      } ]" +
                    "    }, {" +
                    "      \"type\" : \"record\"," +
                    "      \"name\" : \"MyImpl2\"," +
                    "      \"fields\" : [ {" +
                    "        \"name\" : \"data\"," +
                    "        \"type\" : [ \"null\", \"string\" ]," +
                    "        \"default\" : null" +
                    "      } ]" +
                    "    } ]," +
                    "    \"default\" : null" +
                    "  } ]" +
                    "}",
            )
        val myImpl1Schema = ReflectData.AllowNull.get().getSchema(MyImpl1::class.java)
        val nestedRecord = GenericRecordBuilder(myImpl1Schema).set("data", "mydata").build()
        val obj = GenericRecordBuilder(myAvroObjectSchema).set("obj", nestedRecord).build()

        val connectSchema = avroData.toConnectSchema(myAvroObjectSchema)
        val schemaAndValue = avroData.toConnectData(myAvroObjectSchema, obj)!!
        val o = avroData.fromConnectData(schemaAndValue.schema(), schemaAndValue.value())
        Assert.assertEquals(obj, o)
        avroData.fromConnectSchema(connectSchema)
    }

    @Union(MyImpl1::class, MyImpl2::class)
    internal interface MyInterface

    internal class MyImpl1 : MyInterface {
        private val data: String? = null
    }

    internal class MyImpl2 : MyInterface {
        private val data: String? = null
    }

    internal class MyObjectToPersist {
        private val obj: MyInterface? = null
    }

    @Test
    fun testRecordDefault() {
        val myAvroObjectSchema =
            Parser().parse(
                "{" +
                    "  \"type\" : \"record\"," +
                    "  \"name\" : \"MyRecord\"," +
                    "  \"namespace\" : " +
                    "\"com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AdditionalAvroDataTest\"," +
                    "  \"fields\" : [ {" +
                    "    \"name\" : \"obj\"," +
                    "    \"type\" : {" +
                    "      \"name\" : \"obj2\"," +
                    "      \"type\" : \"record\"," +
                    "      \"fields\" : [ {" +
                    "        \"name\" : \"data\"," +
                    "        \"type\" : \"string\"," +
                    "        \"default\" : \"\"" +
                    "      } ]" +
                    "    }," +
                    "    \"default\" : { \"data\" : \"\" }" +
                    "  } ]" +
                    "}",
            )
        val obj = GenericRecordBuilder(myAvroObjectSchema).build()

        val connectSchema = avroData.toConnectSchema(myAvroObjectSchema)
        val schemaAndValue = avroData.toConnectData(myAvroObjectSchema, obj)!!
        val o = avroData.fromConnectData(schemaAndValue.schema(), schemaAndValue.value())
        Assert.assertEquals(obj, o)
        avroData.fromConnectSchema(connectSchema)
    }

    @Test
    fun testFieldRecordEnumDocumentationSchema() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, true)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        avroData = AvroData(avroDataConfig)

        val avroSchema = Parser().parse(File("src/test/avro/RepeatedTypeWithDocFull.avsc"))

        Assert.assertEquals(
            avroSchema.getField("enumField").schema(),
            avroSchema.getField("anotherEnumField").schema(),
        )

        val connectSchema = avroData.toConnectSchema(avroSchema)

        val outputAvroSchema = avroData.fromConnectSchema(connectSchema)

        Assert.assertEquals("record's doc", outputAvroSchema.doc)
        Assert.assertEquals("field's doc", outputAvroSchema.getField("stringField").doc())
        Assert.assertNull(outputAvroSchema.getField("stringField").schema().doc)
        Assert.assertNull(outputAvroSchema.getField("anotherStringField").doc())
        Assert.assertNull(outputAvroSchema.getField("anotherStringField").schema().doc)

        Assert.assertEquals("record field's doc", outputAvroSchema.getField("recordField").doc())
        Assert.assertEquals("nested record's doc", outputAvroSchema.getField("recordField").schema().doc)
        Assert.assertEquals(
            "nested record field's doc",
            outputAvroSchema
                .getField("recordField")
                .schema()
                .getField("nestedRecordField")
                .doc(),
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("recordField")
                .schema()
                .getField("nestedRecordField")
                .schema()
                .doc,
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("recordField")
                .schema()
                .getField("anotherNestedRecordField")
                .doc(),
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("recordField")
                .schema()
                .getField("anotherNestedRecordField")
                .schema()
                .doc,
        )

        Assert.assertEquals("another record field's doc", outputAvroSchema.getField("anotherRecordField").doc())
        Assert.assertEquals("nested record's doc", outputAvroSchema.getField("anotherRecordField").schema().doc)
        Assert.assertEquals(
            "nested record field's doc",
            outputAvroSchema
                .getField("anotherRecordField")
                .schema()
                .getField("nestedRecordField")
                .doc(),
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("anotherRecordField")
                .schema()
                .getField("nestedRecordField")
                .schema()
                .doc,
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("anotherRecordField")
                .schema()
                .getField("anotherNestedRecordField")
                .doc(),
        )
        Assert.assertNull(
            outputAvroSchema
                .getField("anotherRecordField")
                .schema()
                .getField("anotherNestedRecordField")
                .schema()
                .doc,
        )

        Assert.assertNull(outputAvroSchema.getField("recordFieldWithoutDoc").doc())

        Assert.assertNull(outputAvroSchema.getField("doclessRecordField").doc())
        Assert.assertNull(outputAvroSchema.getField("doclessRecordField").schema().doc)
        Assert.assertEquals(
            "docless record field's doc",
            outputAvroSchema.getField("doclessRecordFieldWithDoc").doc(),
        )
        Assert.assertNull(outputAvroSchema.getField("doclessRecordFieldWithDoc").schema().doc)

        Assert.assertEquals("enum field's doc", outputAvroSchema.getField("enumField").doc())
        Assert.assertEquals("enum's doc", outputAvroSchema.getField("enumField").schema().doc)
        Assert.assertEquals("another enum field's doc", outputAvroSchema.getField("anotherEnumField").doc())
        Assert.assertEquals("enum's doc", outputAvroSchema.getField("anotherEnumField").schema().doc)

        Assert.assertNull(outputAvroSchema.getField("doclessEnumField").doc())
        Assert.assertNull(outputAvroSchema.getField("diffEnumField").doc())
        Assert.assertEquals("diffEnum's doc", outputAvroSchema.getField("diffEnumField").schema().doc)

        // Schema equality is mandatory (see issue #1042)
        Assert.assertEquals(
            outputAvroSchema.getField("stringField").schema(),
            outputAvroSchema.getField("anotherStringField").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("recordField").schema(),
            outputAvroSchema.getField("anotherRecordField").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("recordField").schema(),
            outputAvroSchema.getField("recordFieldWithoutDoc").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("doclessRecordField").schema(),
            outputAvroSchema.getField("doclessRecordFieldWithDoc").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("enumField").schema(),
            outputAvroSchema.getField("anotherEnumField").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("enumField").schema(),
            outputAvroSchema.getField("doclessEnumField").schema(),
        )
    }

    @Test
    fun testRepeatedTypeWithDefault() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, true)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        avroData = AvroData(avroDataConfig)

        val avroSchema = Parser().parse(File("src/test/avro/RepeatedTypeWithDefault.avsc"))

        val connectSchema = avroData.toConnectSchema(avroSchema)

        val outputAvroSchema = avroData.fromConnectSchema(connectSchema)

        Assert.assertEquals("field's default", outputAvroSchema.getField("stringField").defaultVal())
        Assert.assertEquals(null, outputAvroSchema.getField("anotherStringField").defaultVal())

        Assert.assertEquals("ONE", outputAvroSchema.getField("enumField").defaultVal())
        Assert.assertEquals("TWO", outputAvroSchema.getField("anotherEnumField").defaultVal())

        Assert.assertEquals("B", outputAvroSchema.getField("enumFieldWithDiffDefault").defaultVal())
        Assert.assertEquals("A", outputAvroSchema.getField("enumFieldWithDiffDefault").schema().enumDefault)

        Assert.assertEquals(9.18f, outputAvroSchema.getField("floatField").defaultVal())

        Assert.assertEquals(
            outputAvroSchema.getField("enumField").schema(),
            outputAvroSchema.getField("anotherEnumField").schema(),
        )
        Assert.assertEquals(
            outputAvroSchema.getField("stringField").schema(),
            outputAvroSchema.getField("anotherStringField").schema(),
        )
    }
}
