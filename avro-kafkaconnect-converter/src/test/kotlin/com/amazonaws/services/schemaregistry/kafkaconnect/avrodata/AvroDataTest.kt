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

import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.AVRO_ENUM_DOC_PREFIX_PROP
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.AVRO_FIELD_DEFAULT_FLAG_PROP
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.AVRO_FIELD_DOC_PREFIX_PROP
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.AVRO_TYPE_ENUM
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.CONNECT_INTERNAL_TYPE_NAME
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.CONNECT_NAME_PROP
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.KEY_FIELD
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.MAP_ENTRY_TYPE_NAME
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.NAMESPACE
import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroData.Companion.VALUE_FIELD
import com.connect.avro.EnumUnion
import com.connect.avro.UserType
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import foo.bar.EnumTest
import foo.bar.Kind
import org.apache.avro.LogicalTypes
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.util.Utf8
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.instanceOf
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.core.IsEqual
import org.junit.Assert
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.powermock.reflect.Whitebox
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Calendar
import java.util.GregorianCalendar
import java.util.TimeZone

class AvroDataTest {
    private var avroData = AvroData(2)

    // Connect -> Avro

    @Test
    fun testFromConnectBoolean() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType()
        checkNonRecordConversion(avroSchema, true, Schema.BOOLEAN_SCHEMA, true, avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_BOOLEAN_SCHEMA)
    }

    @Test
    fun testFromConnectByte() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.type", "int8")
        checkNonRecordConversion(avroSchema, 12, Schema.INT8_SCHEMA, 12.toByte(), avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_INT8_SCHEMA)
    }

    @Test
    fun testFromConnectShort() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.type", "int16")
        checkNonRecordConversion(avroSchema, 12, Schema.INT16_SCHEMA, 12.toShort(), avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_INT16_SCHEMA)
    }

    @Test
    fun testFromConnectInteger() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        checkNonRecordConversion(avroSchema, 12, Schema.INT32_SCHEMA, 12, avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_INT32_SCHEMA)
    }

    @Test
    fun testFromConnectLong() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        checkNonRecordConversion(avroSchema, 12L, Schema.INT64_SCHEMA, 12L, avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_INT64_SCHEMA)
    }

    @Test
    fun testFromConnectFloat() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().floatType()
        checkNonRecordConversion(avroSchema, 12.2f, Schema.FLOAT32_SCHEMA, 12.2f, avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT32_SCHEMA)
    }

    @Test
    fun testFromConnectDouble() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType()
        checkNonRecordConversion(avroSchema, 12.2, Schema.FLOAT64_SCHEMA, 12.2, avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_FLOAT64_SCHEMA)
    }

    @Test
    fun testFromConnectBytes() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        checkNonRecordConversion(
            avroSchema,
            ByteBuffer.wrap("foo".toByteArray()),
            Schema.BYTES_SCHEMA,
            "foo".toByteArray(),
            avroData,
        )

        checkNonRecordConversionNull(Schema.OPTIONAL_BYTES_SCHEMA)
    }

    @Test
    fun testFromConnectString() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().stringType()
        checkNonRecordConversion(avroSchema, "string", Schema.STRING_SCHEMA, "string", avroData)

        checkNonRecordConversionNull(Schema.OPTIONAL_STRING_SCHEMA)
    }

    @Test
    fun testFromConnectEnum() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()
        val avroData = AvroData(avroDataConfig)

        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .enumeration("enum")
                .symbols("one", "two", "three")
        val avroObj = GenericData.EnumSymbol(avroSchema, "one")

        val connectPropsMap =
            mapOf(
                "connect.enum.doc" to "null",
                "com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Enum" to "enum",
                "com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Enum.one" to "one",
                "com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Enum.two" to "two",
                "com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Enum.three" to "three",
            )
        avroSchema.addProp("connect.parameters", connectPropsMap)
        avroSchema.addProp("connect.name", "enum")
        val schemaAndValue = avroData.toConnectData(avroSchema, avroObj)!!
        checkNonRecordConversion(avroSchema, avroObj, schemaAndValue.schema(), schemaAndValue.value(), avroData)
    }

    @Test
    fun testFromConnectMapWithStringKey() {
        val schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
        val expected =
            org.apache.avro.SchemaBuilder
                .map()
                .values(org.apache.avro.SchemaBuilder.builder().intType())
        assertThat(avroData.fromConnectSchema(schema), equalTo(expected))
    }

    @Test
    fun testFromConnectMapWithOptionalKey() {
        val schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
        val expected =
            org.apache.avro.SchemaBuilder.array().items(
                org.apache.avro.SchemaBuilder
                    .record("$NAMESPACE.$MAP_ENTRY_TYPE_NAME")
                    .fields()
                    .optionalString(KEY_FIELD)
                    .requiredInt(VALUE_FIELD)
                    .endRecord(),
            )

        assertThat(avroData.fromConnectSchema(schema), equalTo(expected))
    }

    @Test
    fun testFromConnectMapWithNonStringKey() {
        val schema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
        val expected =
            org.apache.avro.SchemaBuilder.array().items(
                org.apache.avro.SchemaBuilder
                    .record("$NAMESPACE.$MAP_ENTRY_TYPE_NAME")
                    .fields()
                    .requiredInt(KEY_FIELD)
                    .requiredInt(VALUE_FIELD)
                    .endRecord(),
            )

        assertThat(avroData.fromConnectSchema(schema), equalTo(expected))
    }

    @Test
    fun testFromNamedConnectMapWithNonStringKey() {
        assertThat(avroData.fromConnectSchema(NAMED_MAP_SCHEMA), equalTo(NAMED_AVRO_MAP_SCHEMA))
    }

    @Test
    fun testFromConnectComplex() {
        val schema =
            SchemaBuilder
                .struct()
                .field("int8", SchemaBuilder.int8().defaultValue(2.toByte()).doc("int8 field").build())
                .field("int16", Schema.INT16_SCHEMA)
                .field("int32", Schema.INT32_SCHEMA)
                .field("int64", Schema.INT64_SCHEMA)
                .field("float32", Schema.FLOAT32_SCHEMA)
                .field("float64", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
                .build()
        val struct =
            Struct(schema)
                .put("int8", 12.toByte())
                .put("int16", 12.toShort())
                .put("int32", 12)
                .put("int64", 12L)
                .put("float32", 12.2f)
                .put("float64", 12.2)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", ByteBuffer.wrap("foo".toByteArray()))
                .put("array", listOf("a", "b", "c"))
                .put("map", mapOf("field" to 1))
                .put("mapNonStringKeys", mapOf(1 to 1))

        val convertedRecord = avroData.fromConnectData(schema, struct)

        val complexMapElementSchema =
            org.apache.avro.SchemaBuilder
                .record("MapEntry")
                .namespace("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata")
                .fields()
                .requiredInt("key")
                .requiredInt("value")
                .endRecord()

        // One field has some extra data set on it to ensure it gets passed through via the fields
        // config
        val int8Schema = org.apache.avro.SchemaBuilder.builder().intType()
        int8Schema.addProp("connect.doc", "int8 field")
        int8Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(2))
        int8Schema.addProp("connect.type", "int8")
        val int16Schema = org.apache.avro.SchemaBuilder.builder().intType()
        int16Schema.addProp("connect.type", "int16")
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .record(AvroData.DEFAULT_SCHEMA_NAME)
                .namespace(AvroData.NAMESPACE) // default values
                .fields()
                .name("int8")
                .type(int8Schema)
                .withDefault(2)
                .name("int16")
                .type(int16Schema)
                .noDefault()
                .requiredInt("int32")
                .requiredLong("int64")
                .requiredFloat("float32")
                .requiredDouble("float64")
                .requiredBoolean("boolean")
                .requiredString("string")
                .requiredBytes("bytes")
                .name("array")
                .type()
                .array()
                .items()
                .stringType()
                .noDefault()
                .name("map")
                .type()
                .map()
                .values()
                .intType()
                .noDefault()
                .name("mapNonStringKeys")
                .type()
                .array()
                .items(complexMapElementSchema)
                .noDefault()
                .endRecord()
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("int16", 12)
                .set("int32", 12)
                .set("int64", 12L)
                .set("float32", 12.2f)
                .set("float64", 12.2)
                .set("boolean", true)
                .set("string", "foo")
                .set("bytes", ByteBuffer.wrap("foo".toByteArray()))
                .set("array", listOf("a", "b", "c"))
                .set("map", mapOf("field" to 1))
                .set(
                    "mapNonStringKeys",
                    listOf(
                        GenericRecordBuilder(complexMapElementSchema)
                            .set(AvroData.KEY_FIELD, 1)
                            .set(AvroData.VALUE_FIELD, 1)
                            .build(),
                    ),
                ).build()

        assertEquals(avroSchema, (convertedRecord as GenericRecord).schema)
        assertEquals(avroRecord, convertedRecord)
    }

    @Test
    fun testFromConnectComplexWithDefaults() {
        val dateDefVal = 100
        val timeDefVal = 1000 * 60 * 60 * 2
        val tsDefVal = 1000L * 60 * 60 * 24 * 365 + 100
        val dateDef = Date.toLogical(Date.SCHEMA, dateDefVal)
        val timeDef = Time.toLogical(Time.SCHEMA, timeDefVal)
        val tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal)
        val decimalDef = BigDecimal(BigInteger.valueOf(314159L), 5)
        val decimalDefVal = decimalDef.unscaledValue().toByteArray()

        val schema =
            SchemaBuilder
                .struct()
                .field("int8", SchemaBuilder.int8().defaultValue(2.toByte()).doc("int8 field").build())
                .field("int16", SchemaBuilder.int16().defaultValue(12.toShort()).doc("int16 field").build())
                .field("int32", SchemaBuilder.int32().defaultValue(12).doc("int32 field").build())
                .field("int64", SchemaBuilder.int64().defaultValue(12L).doc("int64 field").build())
                .field("float32", SchemaBuilder.float32().defaultValue(12.2f).doc("float32 field").build())
                .field("float64", SchemaBuilder.float64().defaultValue(12.2).doc("float64 field").build())
                .field("boolean", SchemaBuilder.bool().defaultValue(true).doc("bool field").build())
                .field("string", SchemaBuilder.string().defaultValue("foo").doc("string field").build())
                .field(
                    "bytes",
                    SchemaBuilder
                        .bytes()
                        .defaultValue(ByteBuffer.wrap("foo".toByteArray()))
                        .doc("bytes field")
                        .build(),
                ).field(
                    "array",
                    SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(listOf("a", "b", "c")).build(),
                ).field(
                    "map",
                    SchemaBuilder
                        .map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                        .defaultValue(mapOf("field" to 1))
                        .build(),
                ).field("date", Date.builder().defaultValue(dateDef).doc("date field").build())
                .field("time", Time.builder().defaultValue(timeDef).doc("time field").build())
                .field("ts", Timestamp.builder().defaultValue(tsDef).doc("ts field").build())
                .field("decimal", Decimal.builder(5).defaultValue(decimalDef).doc("decimal field").build())
                .build()
        // leave the struct empty so that only defaults are used
        val struct =
            Struct(schema)
                .put("int8", 2.toByte())
                .put("int16", 12.toShort())
                .put("int32", 12)
                .put("int64", 12L)
                .put("float32", 12.2f)
                .put("float64", 12.2)
                .put("boolean", true)
                .put("string", "foo")
                .put("bytes", ByteBuffer.wrap("foo".toByteArray()))
                .put("array", listOf("a", "b", "c"))
                .put("map", mapOf("field" to 1))
                .put("date", dateDef)
                .put("time", timeDef)
                .put("ts", tsDef)
                .put("decimal", decimalDef)

        // Define the expected Avro schema
        val complexMapElementSchema =
            org.apache.avro.SchemaBuilder
                .record("MapEntry")
                .namespace("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata")
                .fields()
                .requiredInt("key")
                .requiredInt("value")
                .endRecord()

        val int8Schema = org.apache.avro.SchemaBuilder.builder().intType()
        int8Schema.addProp("connect.doc", "int8 field")
        int8Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(2))
        int8Schema.addProp("connect.type", "int8")
        val int16Schema = org.apache.avro.SchemaBuilder.builder().intType()
        int16Schema.addProp("connect.doc", "int16 field")
        int16Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12.toShort()).intValue())
        int16Schema.addProp("connect.type", "int16")
        val int32Schema = org.apache.avro.SchemaBuilder.builder().intType()
        int32Schema.addProp("connect.doc", "int32 field")
        int32Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12))
        val int64Schema = org.apache.avro.SchemaBuilder.builder().longType()
        int64Schema.addProp("connect.doc", "int64 field")
        int64Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12L))
        val float32Schema = org.apache.avro.SchemaBuilder.builder().floatType()
        float32Schema.addProp("connect.doc", "float32 field")
        float32Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12.2f))
        val float64Schema = org.apache.avro.SchemaBuilder.builder().doubleType()
        float64Schema.addProp("connect.doc", "float64 field")
        float64Schema.addProp("connect.default", JsonNodeFactory.instance.numberNode(12.2))
        val boolSchema = org.apache.avro.SchemaBuilder.builder().booleanType()
        boolSchema.addProp("connect.doc", "bool field")
        boolSchema.addProp("connect.default", JsonNodeFactory.instance.booleanNode(true))
        val stringSchema = org.apache.avro.SchemaBuilder.builder().stringType()
        stringSchema.addProp("connect.doc", "string field")
        stringSchema.addProp("connect.default", JsonNodeFactory.instance.textNode("foo"))
        val bytesSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        bytesSchema.addProp("connect.doc", "bytes field")
        bytesSchema.addProp(
            "connect.default",
            JsonNodeFactory.instance.textNode(String("foo".toByteArray(), StandardCharsets.ISO_8859_1)),
        )

        val dateSchema = org.apache.avro.SchemaBuilder.builder().intType()
        dateSchema.addProp("connect.doc", "date field")
        dateSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(dateDefVal))
        dateSchema.addProp(AvroData.CONNECT_NAME_PROP, Date.LOGICAL_NAME)
        dateSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1)
        // this is the new and correct way to set logical type
        LogicalTypes.date().addToSchema(dateSchema)
        // this is the old and wrong way to set logical type
        // leave the line here for back compatibility
        dateSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE)

        val timeSchema = org.apache.avro.SchemaBuilder.builder().intType()
        timeSchema.addProp("connect.doc", "time field")
        timeSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(timeDefVal))
        timeSchema.addProp(AvroData.CONNECT_NAME_PROP, Time.LOGICAL_NAME)
        timeSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1)
        // this is the new and correct way to set logical type
        LogicalTypes.timeMillis().addToSchema(timeSchema)
        // this is the old and wrong way to set logical type
        // leave the line here for back compatibility
        timeSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS)

        val tsSchema = org.apache.avro.SchemaBuilder.builder().longType()
        tsSchema.addProp("connect.doc", "ts field")
        tsSchema.addProp("connect.default", JsonNodeFactory.instance.numberNode(tsDefVal))
        tsSchema.addProp(AvroData.CONNECT_NAME_PROP, Timestamp.LOGICAL_NAME)
        tsSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1)
        // this is the new and correct way to set logical type
        LogicalTypes.timestampMillis().addToSchema(tsSchema)
        // this is the old and wrong way to set logical type
        // leave the line here for back compatibility
        tsSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS)

        val decimalSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        decimalSchema.addProp("scale", 5)
        decimalSchema.addProp("precision", 64)
        decimalSchema.addProp("connect.doc", "decimal field")
        decimalSchema.addProp(AvroData.CONNECT_VERSION_PROP, 1)
        decimalSchema.addProp(
            "connect.default",
            JsonNodeFactory.instance.textNode(String(decimalDefVal, StandardCharsets.ISO_8859_1)),
        )
        decimalSchema.addProp("connect.parameters", parameters("scale", "5"))
        decimalSchema.addProp(AvroData.CONNECT_NAME_PROP, Decimal.LOGICAL_NAME)
        // this is the new and correct way to set logical type
        LogicalTypes.decimal(64, 5).addToSchema(decimalSchema)
        // this is the old and wrong way to set logical type
        // leave the line here for back compatibility
        decimalSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL)

        val arraySchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items()
                .stringType()
        val arrayNode = JsonNodeFactory.instance.arrayNode()
        arrayNode.add("a")
        arrayNode.add("b")
        arrayNode.add("c")
        arraySchema.addProp("connect.default", arrayNode)

        val mapSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .map()
                .values()
                .intType()
        val mapNode = JsonNodeFactory.instance.objectNode()
        mapNode.put("field", 1)
        mapSchema.addProp("connect.default", mapNode)

        val nonStringMapSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items(complexMapElementSchema)
        val nonStringMapNode = JsonNodeFactory.instance.arrayNode()
        nonStringMapNode.add(JsonNodeFactory.instance.numberNode(1))
        nonStringMapNode.add(JsonNodeFactory.instance.numberNode(1))
        val nonStringMapArrayNode = JsonNodeFactory.instance.arrayNode()
        nonStringMapArrayNode.add(nonStringMapNode)
        nonStringMapSchema.addProp("connect.default", nonStringMapArrayNode)

        val avroSchema =
            org.apache.avro.SchemaBuilder
                .record(AvroData.DEFAULT_SCHEMA_NAME)
                .namespace(AvroData.NAMESPACE) // default values
                .fields()
                .name("int8")
                .type(int8Schema)
                .withDefault(2)
                .name("int16")
                .type(int16Schema)
                .withDefault(12)
                .name("int32")
                .type(int32Schema)
                .withDefault(12)
                .name("int64")
                .type(int64Schema)
                .withDefault(12L)
                .name("float32")
                .type(float32Schema)
                .withDefault(12.2f)
                .name("float64")
                .type(float64Schema)
                .withDefault(12.2)
                .name("boolean")
                .type(boolSchema)
                .withDefault(true)
                .name("string")
                .type(stringSchema)
                .withDefault("foo")
                .name("bytes")
                .type(bytesSchema)
                .withDefault(ByteBuffer.wrap("foo".toByteArray()))
                .name("array")
                .type(arraySchema)
                .withDefault(listOf("a", "b", "c"))
                .name("map")
                .type(mapSchema)
                .withDefault(mapOf("field" to 1))
                .name("date")
                .type(dateSchema)
                .withDefault(dateDefVal)
                .name("time")
                .type(timeSchema)
                .withDefault(timeDefVal)
                .name("ts")
                .type(tsSchema)
                .withDefault(tsDefVal)
                .name("decimal")
                .type(decimalSchema)
                .withDefault(ByteBuffer.wrap(decimalDefVal))
                .endRecord()
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int8", 2)
                .set("int16", 12)
                .set("int32", 12)
                .set("int64", 12L)
                .set("float32", 12.2f)
                .set("float64", 12.2)
                .set("boolean", true)
                .set("string", "foo")
                .set("bytes", ByteBuffer.wrap("foo".toByteArray()))
                .set("array", listOf("a", "b", "c"))
                .set("map", mapOf("field" to 1))
                .set("date", dateDefVal)
                .set("time", timeDefVal)
                .set("ts", tsDefVal)
                .set("decimal", decimalDefVal)
                .build()

        var schemaAndValue = SchemaAndValue(schema, struct)
        schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue)
        schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue)
        schemaAndValue = convertToConnect(avroSchema, avroRecord, schemaAndValue)
        assertNotNull(schemaAndValue)
    }

    private fun convertToConnect(
        expectedAvroSchema: org.apache.avro.Schema,
        expectedAvroRecord: GenericRecord,
        connectSchemaAndValue: SchemaAndValue,
    ): SchemaAndValue {
        val convertedRecord =
            avroData.fromConnectData(
                connectSchemaAndValue.schema(),
                connectSchemaAndValue.value(),
            )

        val convertedAvroRecord = convertedRecord as GenericRecord
        assertSchemaEquals(expectedAvroSchema, convertedAvroRecord.schema)
        assertSchemaEquals(expectedAvroRecord.schema, convertedAvroRecord.schema)

        // This doesn't work because the long field's default value is an integer
        // assertEquals(avroRecord, convertedRecord);
        // We've already checked the schemas, so we just need to check the record field values
        for (field in expectedAvroSchema.fields) {
            val actual = convertedAvroRecord.get(field.name())
            val expected = expectedAvroRecord.get(field.name())
            assertValueEquals(expected, actual)
        }

        val schemaAndValue =
            avroData.toConnectData(
                convertedAvroRecord.schema,
                convertedRecord,
            )!!
        assertEquals(connectSchemaAndValue.schema(), schemaAndValue.schema())
        assertEquals(connectSchemaAndValue.value(), schemaAndValue.value())
        return schemaAndValue
    }

    @Test
    fun testFromConnectOptionalWithDefaultNull() {
        val schema =
            SchemaBuilder
                .struct()
                .field("optionalBool", SchemaBuilder.bool().optional().defaultValue(null).build())
                .build()
        val avroSchema = avroData.fromConnectSchema(schema)
        val expectedAvroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("ConnectDefault")
                .namespace("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata")
                .fields()
                .name("optionalBool")
                .type(
                    org.apache.avro.SchemaBuilder
                        .builder()
                        .unionOf()
                        .nullType()
                        .and()
                        .booleanType()
                        .endUnion(),
                ).withDefault(null)
                .endRecord()

        assertEquals(expectedAvroSchema, avroSchema)

        val struct =
            Struct(schema)
                .put("optionalBool", true)
        val convertedRecord = avroData.fromConnectData(schema, struct)
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("optionalBool", true)
                .build()

        assertEquals(avroRecord, convertedRecord)
    }

    @Test
    fun testFromConnectOptionalAnonymousStruct() {
        val schema =
            SchemaBuilder
                .struct()
                .optional()
                .field("int32", Schema.INT32_SCHEMA)
                .build()

        val struct = Struct(schema).put("int32", 12)

        val convertedRecord = avroData.fromConnectData(schema, struct)

        assertThat(convertedRecord, instanceOf(GenericRecord::class.java))
        assertThat((convertedRecord as GenericRecord).get("int32"), equalTo<Any>(12))
    }

    @Test
    fun testFromConnectOptionalComplex() {
        val optionalStructSchema =
            SchemaBuilder
                .struct()
                .optional()
                .name("optionalStruct")
                .field("int32", Schema.INT32_SCHEMA)
                .build()

        val schema =
            SchemaBuilder
                .struct()
                .field("int32", Schema.INT32_SCHEMA)
                .field("optionalStruct", optionalStructSchema)
                .field("optionalArray", SchemaBuilder.array(Schema.INT32_SCHEMA).optional().build())
                .field(
                    "optionalMap",
                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
                ).field(
                    "optionalMapNonStringKeys",
                    SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).optional().build(),
                ).build()

        var struct =
            Struct(schema)
                .put("int32", 12)
                .put("optionalStruct", Struct(optionalStructSchema).put("int32", 12))
                .put("optionalArray", listOf(12, 13))
                .put("optionalMap", mapOf("field" to 12))
                .put("optionalMapNonStringKeys", mapOf(123 to 12))

        var convertedRecord = avroData.fromConnectData(schema, struct)

        val structAvroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("optionalStruct")
                .fields()
                .requiredInt("int32")
                .endRecord()

        // Maps with non-string keys get converted into an array of records with key & values fields
        val mapNonStringKeysAvroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record(AvroData.MAP_ENTRY_TYPE_NAME)
                .namespace(AvroData.NAMESPACE)
                .fields()
                .requiredInt(AvroData.KEY_FIELD)
                .requiredInt(AvroData.VALUE_FIELD)
                .endRecord()

        val avroSchema = avroData.fromConnectSchema(schema)

        val avroStruct =
            GenericRecordBuilder(structAvroSchema)
                .set("int32", 12)
                .build()
        val mapNonStringKeysAvroStruct =
            GenericRecordBuilder(mapNonStringKeysAvroSchema)
                .set(AvroData.KEY_FIELD, 123)
                .set(AvroData.VALUE_FIELD, 12)
                .build()

        val mapNonStringKeys = ArrayList<GenericRecord>()
        mapNonStringKeys.add(mapNonStringKeysAvroStruct)

        var avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int32", 12)
                .set("optionalStruct", avroStruct)
                .set("optionalArray", listOf(12, 13))
                .set("optionalMap", mapOf("field" to 12))
                .set("optionalMapNonStringKeys", mapNonStringKeys)
                .build()

        assertEquals(avroRecord, convertedRecord)

        // Now check for null values
        struct =
            Struct(schema)
                .put("int32", 12)
                .put("optionalStruct", null)
                .put("optionalArray", null)
                .put("optionalMap", null)
                .put("optionalMapNonStringKeys", null)

        convertedRecord = avroData.fromConnectData(schema, struct)

        avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int32", 12)
                .set("optionalStruct", null)
                .set("optionalArray", null)
                .set("optionalMap", null)
                .set("optionalMapNonStringKeys", null)
                .build()

        assertEquals(avroRecord, convertedRecord)
    }

    @Test
    fun testFromConnectOptionalPrimitiveWithMetadata() {
        val schema =
            SchemaBuilder
                .string()
                .doc("doc")
                .defaultValue("foo")
                .name("com.amazonaws.services.schemaregistry.stringtype")
                .version(2)
                .optional()
                .parameter("foo", "bar")
                .parameter("baz", "baz")
                .build()

        // Missing some metadata, used to validate missing properties on the Avro schema will cause
        // schemas to be considered not equal
        val wrongAvroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .stringType()
                .and()
                .nullType()
                .endUnion()

        // The complete schema
        val avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType()
        avroStringSchema.addProp("connect.name", "com.amazonaws.services.schemaregistry.stringtype")
        avroStringSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(2))
        avroStringSchema.addProp("connect.doc", "doc")
        avroStringSchema.addProp("connect.default", "foo")
        val params = JsonNodeFactory.instance.objectNode()
        params.put("foo", "bar")
        params.put("baz", "baz")
        avroStringSchema.addProp("connect.parameters", params)
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .type(avroStringSchema)
                .and()
                .nullType()
                .endUnion()

        val converted = checkNonRecordConversion(avroSchema, "string", schema, "string", avroData)
        assertNotEquals(wrongAvroSchema, converted.getSchema())

        // Validate null is correctly translated to null again
        checkNonRecordConversionNull(schema)
    }

    @Test
    fun testFromConnectRecordWithMetadata() {
        val schema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.test.TestSchema")
                .version(12)
                .doc("doc")
                .field("int32", Schema.INT32_SCHEMA)
                .build()
        val struct =
            Struct(schema)
                .put("int32", 12)

        val convertedRecord = avroData.fromConnectData(schema, struct)

        val avroSchema =
            org.apache.avro.SchemaBuilder
                .record("TestSchema")
                .namespace("com.amazonaws.services.schemaregistry.test")
                .fields()
                .requiredInt("int32")
                .endRecord()
        avroSchema.addProp("connect.name", "com.amazonaws.services.schemaregistry.test.TestSchema")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(12))
        avroSchema.addProp("connect.doc", "doc")
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int32", 12)
                .build()

        assertEquals(avroSchema, (convertedRecord as GenericRecord).schema)
        assertEquals(avroRecord, convertedRecord)
    }

    // test for new way of logical type handling
    @Test
    fun testFromConnectLogicalDecimalNew() {
        val avroSchema = createDecimalSchema(true, 64)
        checkNonRecordConversionNew(
            avroSchema,
            ByteBuffer.wrap(TEST_DECIMAL_BYTES),
            Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build(),
            TEST_DECIMAL,
            avroData,
        )
        checkNonRecordConversionNull(Decimal.builder(2).optional().build())
    }

    // test for new way of logical type handling
    @Test
    fun testFromConnectLogicalDateNew() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE)
        LogicalTypes.date().addToSchema(avroSchema)
        checkNonRecordConversionNew(
            avroSchema,
            10000,
            Date.SCHEMA,
            EPOCH_PLUS_TEN_THOUSAND_DAYS.time,
            avroData,
        )
    }

    // test for new way of logical type handling
    @Test
    fun testFromConnectLogicalTimeNew() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS)
        LogicalTypes.timeMillis().addToSchema(avroSchema)
        checkNonRecordConversionNew(
            avroSchema,
            10000,
            Time.SCHEMA,
            EPOCH_PLUS_TEN_THOUSAND_MILLIS.time,
            avroData,
        )
    }

    // test for new way of logical type handling
    @Test
    fun testFromConnectLogicalTimestampNew() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS)
        LogicalTypes.timestampMillis().addToSchema(avroSchema)
        val date = java.util.Date()
        checkNonRecordConversionNew(avroSchema, date.time, Timestamp.SCHEMA, date, avroData)
    }

    // Test to ensure that a decimal with a scale greater than its precision can be safely handled
    @Test
    fun testFromConnectLogicalDecimalScaleGreaterThanPrecision() {
        val precision = 5
        val scale = 7
        val testDecimal = BigDecimal(BigInteger("12358"), scale)
        val avroSchema = createDecimalSchema(true, precision, scale)
        checkNonRecordConversion(
            avroSchema,
            ByteBuffer.wrap(testDecimal.unscaledValue().toByteArray()),
            Decimal
                .builder(scale)
                .parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, precision.toString())
                .build(),
            testDecimal,
            avroData,
        )
        checkNonRecordConversionNull(Decimal.builder(scale).optional().build())
    }

    // test for old way of logical type handling
    @Test
    fun testFromConnectLogicalDecimal() {
        val avroSchema = createDecimalSchema(true, 64)
        checkNonRecordConversion(
            avroSchema,
            ByteBuffer.wrap(TEST_DECIMAL_BYTES),
            Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64").build(),
            TEST_DECIMAL,
            avroData,
        )
        checkNonRecordConversionNull(Decimal.builder(2).optional().build())
    }

    // test for old way of logical type handling
    @Test
    fun testFromConnectLogicalDate() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE)
        checkNonRecordConversion(
            avroSchema,
            10000,
            Date.SCHEMA,
            EPOCH_PLUS_TEN_THOUSAND_DAYS.time,
            avroData,
        )
    }

    // test for old way of logical type handling
    @Test
    fun testFromConnectLogicalTime() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS)
        checkNonRecordConversion(
            avroSchema,
            10000,
            Time.SCHEMA,
            EPOCH_PLUS_TEN_THOUSAND_MILLIS.time,
            avroData,
        )
    }

    // test for old way of logical type handling
    @Test
    fun testFromConnectLogicalTimestamp() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS)
        val date = java.util.Date()
        checkNonRecordConversion(avroSchema, date.time, Timestamp.SCHEMA, date, avroData)
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaPrimitive() {
        avroData.fromConnectData(Schema.OPTIONAL_BOOLEAN_SCHEMA, 12)
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaPrimitiveRequired() {
        avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, null)
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaArray() {
        avroData.fromConnectData(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build(), listOf(12))
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaMapWithStringKeyMismatchKey() {
        avroData.fromConnectData(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
            mapOf(true to 12),
        )
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaMapWithStringKeyMismatchValue() {
        avroData.fromConnectData(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
            mapOf("foobar" to 12L),
        )
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaMapWithNonStringKeyMismatchKey() {
        avroData.fromConnectData(
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
            mapOf(true to 12),
        )
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaMapWithNonStringKeyMismatchValue() {
        avroData.fromConnectData(
            SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(),
            mapOf(12 to 12L),
        )
    }

    @Test(expected = DataException::class)
    fun testFromConnectMismatchSchemaRecord() {
        val firstSchema =
            SchemaBuilder
                .struct()
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .build()
        val secondSchema =
            SchemaBuilder
                .struct()
                .field("foo", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build()

        avroData.fromConnectData(firstSchema, Struct(secondSchema).put("foo", null))
    }

    @Test(expected = DataException::class)
    fun testToConnectRecordWithIllegalNullValue() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .requiredString("string")
                .endRecord()
        val avroRecord = GenericRecordBuilder(avroSchema).set("string", "some value").build()
        avroRecord.put("string", null)
        avroData.toConnectData(avroSchema, avroRecord)
    }

    @Test
    fun testFromConnectSchemaless() {
        // Null has special handling. We do *not* want to get back ANYTHING_SCHEMA because we're going
        // to discard it anyway. We should just be passing through the null value.
        val nullConverted = avroData.fromConnectData(null, null)
        assertNull(nullConverted)

        checkNonRecordConversionNull(null)

        val avroIntRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("int", 12)
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, 12.toByte(), avroData)
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, 12.toShort(), avroData)
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroIntRecord, null, 12, avroData)

        val avroLongRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("long", 12L)
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroLongRecord, null, 12L, avroData)

        val avroFloatRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("float", 12.2f)
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroFloatRecord, null, 12.2f, avroData)

        val avroDoubleRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("double", 12.2)
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroDoubleRecord, null, 12.2, avroData)

        val avroBooleanRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("boolean", true)
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroBooleanRecord, null, true, avroData)

        val avroStringRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("string", "teststring")
                .build()
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroStringRecord, null, "teststring", avroData)

        val avroNullRecord = GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build()
        val avroArrayRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("array", listOf(avroIntRecord, avroStringRecord, avroNullRecord))
                .build()
        checkNonRecordConversion(
            AvroData.ANYTHING_SCHEMA,
            avroArrayRecord,
            null,
            listOf(12, "teststring", null),
            avroData,
        )

        val avroMapEntry =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
                .set("key", avroIntRecord)
                .set("value", avroStringRecord)
                .build()
        val avroMapEntryNull =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
                .set("key", GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).set("int", 13).build())
                .set("value", avroNullRecord)
                .build()
        val avroMapRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("map", listOf(avroMapEntry, avroMapEntryNull))
                .build()
        val convertedMap = HashMap<Any?, Any?>()
        convertedMap[12] = "teststring"
        convertedMap[13] = null
        checkNonRecordConversion(AvroData.ANYTHING_SCHEMA, avroMapRecord, null, convertedMap, avroData)
    }

    @Test
    fun testCacheSchemaFromConnectConversion() {
        val cache: Cache<org.apache.avro.Schema, Schema> =
            Whitebox.getInternalState(avroData, "fromConnectSchemaCache")
        assertEquals(0, cache.size())

        avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, true)
        assertEquals(1, cache.size())

        avroData.fromConnectData(Schema.BOOLEAN_SCHEMA, true)
        assertEquals(1, cache.size())

        avroData.fromConnectData(Schema.OPTIONAL_BOOLEAN_SCHEMA, true)
        assertEquals(2, cache.size())

        // Should hit limit of cache
        avroData.fromConnectData(Schema.STRING_SCHEMA, "foo")
        assertEquals(2, cache.size())
    }

    @Test
    fun testEnum() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        val avroData = AvroData(avroDataConfig)

        val testModel =
            EnumTest
                .newBuilder()
                .setTestkey("name")
                .setKind(Kind.ONE)
                .build()

        val schemaAndValue = avroData.toConnectData(EnumTest.`SCHEMA$`, testModel)!!
        val schema = schemaAndValue.schema()
        val schemaValue = schemaAndValue.value()

        val value = avroData.fromConnectData(schema, schemaValue) as GenericData.Record
        val userTypeValue = value.get("kind") as GenericContainer
        Assert.assertEquals(userTypeValue.schema.type, org.apache.avro.Schema.Type.ENUM)
    }

    @Test
    fun testEnumUnion() {
        val genericData = GenericData.get()
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        val avroData = AvroData(avroDataConfig)

        val testModel =
            EnumUnion
                .newBuilder()
                .setUserType(UserType.ANONYMOUS)
                .build()

        val schemaAndValue = avroData.toConnectData(EnumUnion.`SCHEMA$`, testModel)!!
        val schema = schemaAndValue.schema()
        val schemaValue = schemaAndValue.value()

        val value = avroData.fromConnectData(schema, schemaValue) as GenericData.Record

        val userTypeSchema = EnumUnion.`SCHEMA$`.getField("userType").schema()

        val userTypeValue = value.get("userType")

        val unionIndex = genericData.resolveUnion(userTypeSchema, userTypeValue)
        Assert.assertEquals(1, unionIndex)
    }

    @Test
    fun testEnumUnionNullValue() {
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .build()

        val avroData = AvroData(avroDataConfig)

        val testModel =
            EnumUnion
                .newBuilder()
                .setUserType(null)
                .build()

        val schemaAndValue = avroData.toConnectData(EnumUnion.`SCHEMA$`, testModel)!!
        val schema = schemaAndValue.schema()
        val schemaValue = schemaAndValue.value()

        val value = avroData.fromConnectData(schema, schemaValue) as GenericData.Record
        val userTypeValue = value.get("userType")
        Assert.assertNull(userTypeValue)
    }

    // Avro -> Connect. Validate a) all Avro types that convert directly to Avro, b) specialized
    // Avro types where we can convert to a Connect type that doesn't have a corresponding Avro
    // type, and c) Avro types which need specialized transformation because there is no
    // corresponding Connect type.

    // Avro -> Connect: directly corresponding types

    @Test
    fun testToConnectNull() {
        assertNull(avroData.toConnectData(null, null))
    }

    @Test
    fun testToConnectBoolean() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType()
        assertEquals(
            SchemaAndValue(Schema.BOOLEAN_SCHEMA, true),
            avroData.toConnectData(avroSchema, true),
        )
    }

    @Test
    fun testToConnectInt32() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        assertEquals(
            SchemaAndValue(Schema.INT32_SCHEMA, 12),
            avroData.toConnectData(avroSchema, 12),
        )
    }

    @Test
    fun testToConnectInt64() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        assertEquals(
            SchemaAndValue(Schema.INT64_SCHEMA, 12L),
            avroData.toConnectData(avroSchema, 12L),
        )
    }

    @Test
    fun testToConnectFloat32() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().floatType()
        assertEquals(
            SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.0f),
            avroData.toConnectData(avroSchema, 12.0f),
        )
    }

    @Test
    fun testToConnectFloat64() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType()
        assertEquals(
            SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.0),
            avroData.toConnectData(avroSchema, 12.0),
        )
    }

    @Test
    fun testToConnectNullableStringNullvalue() {
        val avroSchema = org.apache.avro.SchemaBuilder.nullable().stringType()
        assertEquals(null, avroData.toConnectData(avroSchema, null))
    }

    @Test
    fun testToConnectNullableString() {
        val avroSchema = org.apache.avro.SchemaBuilder.nullable().stringType()
        assertEquals(
            SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, "teststring"),
            avroData.toConnectData(avroSchema, "teststring"),
        )

        // Avro deserializer allows CharSequence, not just String, and returns Utf8 objects
        assertEquals(
            SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, "teststring"),
            avroData.toConnectData(avroSchema, Utf8("teststring")),
        )
    }

    @Test
    fun testToConnectString() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().stringType()
        assertEquals(
            SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
            avroData.toConnectData(avroSchema, "teststring"),
        )

        // Avro deserializer allows CharSequence, not just String, and returns Utf8 objects
        assertEquals(
            SchemaAndValue(Schema.STRING_SCHEMA, "teststring"),
            avroData.toConnectData(avroSchema, Utf8("teststring")),
        )
    }

    @Test
    fun testToConnectBytes() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        assertEquals(
            SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".toByteArray())),
            avroData.toConnectData(avroSchema, "foo".toByteArray()),
        )

        assertEquals(
            SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".toByteArray())),
            avroData.toConnectData(avroSchema, ByteBuffer.wrap("foo".toByteArray())),
        )
    }

    @Test
    fun testToConnectArray() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items()
                .intType()
        avroSchema.elementType.addProp("connect.type", "int8")
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema = SchemaBuilder.array(Schema.INT8_SCHEMA).build()
        assertEquals(
            SchemaAndValue(schema, listOf(12.toByte(), 13.toByte())),
            avroData.toConnectData(avroSchema, listOf(12, 13)),
        )
    }

    @Test
    fun testToConnectMapStringKeys() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .map()
                .values()
                .intType()
        avroSchema.valueType.addProp("connect.type", "int8")
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build()
        assertEquals(
            SchemaAndValue(schema, mapOf("field" to 12.toByte())),
            avroData.toConnectData(avroSchema, mapOf("field" to 12)),
        )
    }

    @Test
    fun testToConnectRecord() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .requiredInt("int8")
                .requiredString("string")
                .endRecord()
        avroSchema.getField("int8").schema().addProp("connect.type", "int8")
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("string", "sample string")
                .build()
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema =
            SchemaBuilder
                .struct()
                .name("Record")
                .field("int8", Schema.INT8_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .build()
        val struct = Struct(schema).put("int8", 12.toByte()).put("string", "sample string")
        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    @Test
    fun testToConnectRecordWithOptionalValue() {
        testToConnectRecordWithOptional("sample string")
    }

    @Test
    fun testToConnectRecordWithOptionalNullValue() {
        testToConnectRecordWithOptional(null)
    }

    private fun testToConnectRecordWithOptional(value: String?) {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .requiredInt("int8")
                .optionalString("string")
                .endRecord()
        avroSchema.getField("int8").schema().addProp("connect.type", "int8")
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int8", 12)
                .set("string", value)
                .build()
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema =
            SchemaBuilder
                .struct()
                .name("Record")
                .field("int8", Schema.INT8_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .build()
        val struct = Struct(schema).put("int8", 12.toByte()).put("string", value)
        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    @Test
    fun testToConnectRecordWithOptionalArrayValue() {
        testToConnectRecordWithOptionalArray(listOf("test"))
    }

    @Test
    fun testToConnectRecordWithOptionalArrayNullValue() {
        testToConnectRecordWithOptionalArray(null)
    }

    private fun testToConnectRecordWithOptionalArray(value: List<String>?) {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .optionalString("string")
                .name("array")
                .type(
                    org.apache.avro.SchemaBuilder
                        .builder()
                        .nullable()
                        .array()
                        .items()
                        .stringType(),
                ).noDefault()
                .endRecord()
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("string", "xx")
                .set("array", value)
                .build()
        val schema =
            SchemaBuilder
                .struct()
                .name("Record")
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .build()
        val struct = Struct(schema).put("string", "xx").put("array", value)
        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    @Test
    fun testToConnectNestedRecordWithOptionalRecordValue() {
        val avroSchema = nestedRecordAvroSchema()
        val schema = nestedRecordSchema()
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("nestedRecord", GenericRecordBuilder(recordWithStringAvroSchema()).set("string", "xx").build())
                .build()
        val struct = Struct(schema).put("nestedRecord", Struct(recordWithStringSchema()).put("string", "xx"))
        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    @Test
    fun testToConnectNestedRecordWithOptionalRecordNullValue() {
        val avroSchema = nestedRecordAvroSchema()
        val schema = nestedRecordSchema()
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("nestedRecord", null)
                .build()
        val struct = Struct(schema).put("nestedRecord", null)
        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    private fun recordWithStringAvroSchema(): org.apache.avro.Schema = org.apache.avro.SchemaBuilder
        .builder()
        .record("nestedRecord")
        .fields()
        .requiredString("string")
        .endRecord()

    private fun nestedRecordAvroSchema(): org.apache.avro.Schema {
        val optionalRecordAvroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .type(recordWithStringAvroSchema())
                .and()
                .nullType()
                .endUnion()
        return org.apache.avro.SchemaBuilder
            .builder()
            .record("Record")
            .fields()
            .name("nestedRecord")
            .type(optionalRecordAvroSchema)
            .noDefault()
            .endRecord()
    }

    private fun recordWithStringSchema(): Schema = SchemaBuilder
        .struct()
        .optional()
        .name("nestedRecord")
        .field("string", Schema.STRING_SCHEMA)
        .build()

    private fun nestedRecordSchema(): Schema = SchemaBuilder
        .struct()
        .name("Record")
        .field("nestedRecord", recordWithStringSchema())
        .build()

    // Avro -> Connect: Connect logical types

    @Test
    fun testToConnectDecimal() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        val avroParams = JsonNodeFactory.instance.objectNode()
        avroParams.put("scale", "2")
        avroSchema.addProp("connect.parameters", avroParams)
        assertEquals(
            SchemaAndValue(Decimal.schema(2), TEST_DECIMAL),
            avroData.toConnectData(avroSchema, TEST_DECIMAL_BYTES),
        )
    }

    @Test
    fun testToConnectDecimalAvro() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType()
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL)
        avroSchema.addProp("precision", 50)
        avroSchema.addProp("scale", 2)

        val expected =
            SchemaAndValue(
                Decimal.builder(2).parameter(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "50").build(),
                TEST_DECIMAL,
            )

        val actual = avroData.toConnectData(avroSchema, TEST_DECIMAL_BYTES)!!
        assertThat(
            "schema.parameters() does not match.",
            actual.schema().parameters(),
            IsEqual.equalTo(expected.schema().parameters()),
        )
        assertEquals("schema does not match.", expected.schema(), actual.schema())
        assertEquals("value does not match.", expected.value(), actual.value())
    }

    @Test
    fun testToConnectDate() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Date")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        assertEquals(
            SchemaAndValue(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.time),
            avroData.toConnectData(avroSchema, 10000),
        )
    }

    @Test
    fun testToConnectDateAvro() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DATE)
        assertEquals(
            SchemaAndValue(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.time),
            avroData.toConnectData(avroSchema, 10000),
        )
    }

    @Test
    fun testToConnectTime() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Time")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        assertEquals(
            SchemaAndValue(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.time),
            avroData.toConnectData(avroSchema, 10000),
        )
    }

    @Test
    fun testToConnectTimeAvro() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIME_MILLIS)
        assertEquals(
            SchemaAndValue(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.time),
            avroData.toConnectData(avroSchema, 10000),
        )
    }

    @Test
    fun testToConnectTimestamp() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        avroSchema.addProp("connect.name", "org.apache.kafka.connect.data.Timestamp")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
        val date = java.util.Date()
        assertEquals(
            SchemaAndValue(Timestamp.SCHEMA, date),
            avroData.toConnectData(avroSchema, date.time),
        )
    }

    @Test
    fun testToConnectTimestampAvro() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().longType()
        avroSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_TIMESTAMP_MILLIS)
        val date = java.util.Date()
        assertEquals(
            SchemaAndValue(Timestamp.SCHEMA, date),
            avroData.toConnectData(avroSchema, date.time),
        )
    }

    // Avro -> Connect: Connect types with no corresponding Avro type

    @Test
    fun testToConnectInt8() {
        // int8 should have a special annotation and Avro will have decoded an Integer
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.type", "int8")
        assertEquals(
            SchemaAndValue(Schema.INT8_SCHEMA, 12.toByte()),
            avroData.toConnectData(avroSchema, 12),
        )
    }

    @Test
    fun testToConnectInt16() {
        // int16 should have a special annotation and Avro will have decoded an Integer
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroSchema.addProp("connect.type", "int16")
        assertEquals(
            SchemaAndValue(Schema.INT16_SCHEMA, 12.toShort()),
            avroData.toConnectData(avroSchema, 12),
        )
    }

    @Test
    fun testToConnectMapNonStringKeys() {
        // Encoded as array of 2-tuple records. Use key and value types that require conversion to
        // make sure conversion of each element actually occurs. The more verbose construction of the
        // Avro schema avoids reuse of schemas, which is needed since after constructing the schemas
        // we set additional properties on them.
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items()
                .record("MapEntry")
                .namespace("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata")
                .fields()
                .name("key")
                .type(org.apache.avro.SchemaBuilder.builder().intType())
                .noDefault()
                .name("value")
                .type(org.apache.avro.SchemaBuilder.builder().intType())
                .noDefault()
                .endRecord()
        avroSchema.elementType
            .getField("key")
            .schema()
            .addProp("connect.type", "int8")
        avroSchema.elementType
            .getField("value")
            .schema()
            .addProp("connect.type", "int16")
        val record =
            GenericRecordBuilder(avroSchema.elementType)
                .set("key", 12)
                .set("value", 16)
                .build()
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA).build()
        assertEquals(
            SchemaAndValue(schema, mapOf(12.toByte() to 16.toShort())),
            avroData.toConnectData(avroSchema, listOf(record)),
        )
    }

    @Test
    fun testToConnectMapOptionalValue() {
        testToConnectMapOptional("some value")
    }

    @Test
    fun testToConnectMapOptionalNullValue() {
        testToConnectMapOptional(null)
    }

    private fun testToConnectMapOptional(value: String?) {
        // Encoded as array of 2-tuple records. Use key and value types that require conversion to
        // make sure conversion of each element actually occurs. The more verbose construction of the
        // Avro schema avoids reuse of schemas, which is needed since after constructing the schemas
        // we set additional properties on them.
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items()
                .record("MapEntry")
                .namespace("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata")
                .fields()
                .name("key")
                .type(org.apache.avro.SchemaBuilder.builder().intType())
                .noDefault()
                .name("value")
                .type(
                    org.apache.avro.SchemaBuilder
                        .builder()
                        .nullable()
                        .stringType(),
                ).noDefault()
                .endRecord()
        avroSchema.elementType
            .getField("key")
            .schema()
            .addProp("connect.type", "int8")
        val record =
            GenericRecordBuilder(avroSchema.elementType)
                .set("key", 12)
                .set("value", value)
                .build()
        // Use a value type which ensures we test conversion of elements. int8 requires extra
        // conversion steps but keeps the test simple.
        val schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build()
        assertEquals(
            SchemaAndValue(schema, mapOf(12.toByte() to value)),
            avroData.toConnectData(avroSchema, listOf(record)),
        )
    }

    @Test
    fun testToConnectMapWithNamedSchema() {
        assertThat(avroData.toConnectSchema(NAMED_AVRO_MAP_SCHEMA), equalTo(NAMED_MAP_SCHEMA))
    }

    // Avro -> Connect: Avro types with no corresponding Connect type

    @Test(expected = DataException::class)
    fun testToConnectNullType() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().nullType()
        // If we somehow did end up with a null schema and an actual value that let it get past the
        avroData.toConnectData(avroSchema, true)
    }

    @Test
    fun testToConnectFixed() {
        // Our conversion simply loses the fixed size information.
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .fixed("sample")
                .size(4)
        assertEquals(
            SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".toByteArray())),
            avroData.toConnectData(avroSchema, "foob".toByteArray()),
        )

        assertEquals(
            SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".toByteArray())),
            avroData.toConnectData(avroSchema, ByteBuffer.wrap("foob".toByteArray())),
        )

        // test with actual fixed type
        assertEquals(
            SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foob".toByteArray())),
            avroData.toConnectData(avroSchema, GenericData.Fixed(avroSchema, "foob".toByteArray())),
        )
    }

    @Test
    fun testToConnectUnion() {
        // Make sure we handle primitive types and named types properly by using a variety of types
        val avroRecordSchema1 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroRecordSchema2 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test2")
                .namespace("com.amazonaws.services.schemaregistry")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .intType()
                .and()
                .stringType()
                .and()
                .type(avroRecordSchema1)
                .and()
                .type(avroRecordSchema2)
                .endUnion()

        val recordSchema1 =
            SchemaBuilder
                .struct()
                .name("Test1")
                .field("test", Schema.INT32_SCHEMA)
                .optional()
                .build()
        val recordSchema2 =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.Test2")
                .field("test", Schema.INT32_SCHEMA)
                .optional()
                .build()
        val schema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Union")
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Test1", recordSchema1)
                .field("Test2", recordSchema2)
                .build()
        assertEquals(
            SchemaAndValue(schema, Struct(schema).put("int", 12)),
            avroData.toConnectData(avroSchema, 12),
        )
        assertEquals(
            SchemaAndValue(schema, Struct(schema).put("string", "teststring")),
            avroData.toConnectData(avroSchema, "teststring"),
        )

        val schema1Test = Struct(schema).put("Test1", Struct(recordSchema1).put("test", 12))
        val record1Test = GenericRecordBuilder(avroRecordSchema1).set("test", 12).build()
        val schema2Test = Struct(schema).put("Test2", Struct(recordSchema2).put("test", 12))
        val record2Test = GenericRecordBuilder(avroRecordSchema2).set("test", 12).build()
        assertEquals(
            SchemaAndValue(schema, schema1Test),
            avroData.toConnectData(avroSchema, record1Test),
        )
        assertEquals(
            SchemaAndValue(schema, schema2Test),
            avroData.toConnectData(avroSchema, record2Test),
        )
    }

    @Test
    fun testToConnectUnionWithEnhanced() {
        avroData =
            AvroData(
                AvroDataConfig
                    .Builder()
                    .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
                    .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                    .build(),
            )
        // Make sure we handle primitive types and named types properly by using a variety of types
        val avroRecordSchema1 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroRecordSchema2 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test2")
                .namespace("com.amazonaws.services.schemaregistry")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .intType()
                .and()
                .stringType()
                .and()
                .type(avroRecordSchema1)
                .and()
                .type(avroRecordSchema2)
                .endUnion()

        val recordSchema1 =
            SchemaBuilder
                .struct()
                .name("Test1")
                .field("test", Schema.INT32_SCHEMA)
                .optional()
                .build()
        val recordSchema2 =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.Test2")
                .field("test", Schema.INT32_SCHEMA)
                .optional()
                .build()
        val schema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.Union")
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Test1", recordSchema1)
                .field("com.amazonaws.services.schemaregistry.Test2", recordSchema2)
                .build()
        assertEquals(
            SchemaAndValue(schema, Struct(schema).put("int", 12)),
            avroData.toConnectData(avroSchema, 12),
        )
        assertEquals(
            SchemaAndValue(schema, Struct(schema).put("string", "teststring")),
            avroData.toConnectData(avroSchema, "teststring"),
        )

        val schema1Test = Struct(schema).put("Test1", Struct(recordSchema1).put("test", 12))
        val record1Test = GenericRecordBuilder(avroRecordSchema1).set("test", 12).build()
        val schema2Test =
            Struct(schema).put(
                "com.amazonaws.services.schemaregistry.Test2",
                Struct(recordSchema2).put("test", 12),
            )
        val record2Test = GenericRecordBuilder(avroRecordSchema2).set("test", 12).build()
        assertEquals(
            SchemaAndValue(schema, schema1Test),
            avroData.toConnectData(avroSchema, record1Test),
        )
        assertEquals(
            SchemaAndValue(schema, schema2Test),
            avroData.toConnectData(avroSchema, record2Test),
        )
    }

    @Test(expected = DataException::class)
    fun testToConnectUnionRecordConflict() {
        // If the records have the same name but are in different namespaces, we don't support this
        // because Avro field naming is fairly restrictive
        val avroRecordSchema1 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroRecordSchema2 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .namespace("com.amazonaws.services.schemaregistry")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .type(avroRecordSchema1)
                .and()
                .type(avroRecordSchema2)
                .endUnion()

        val recordTest = GenericRecordBuilder(avroRecordSchema1).set("test", 12).build()
        avroData.toConnectData(avroSchema, recordTest)
    }

    @Test
    fun testToConnectUnionRecordConflictWithEnhanced() {
        // If the records have the same name but are in different namespaces,
        // ensure these are handled without throwing exception
        avroData =
            AvroData(
                AvroDataConfig
                    .Builder()
                    .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 2)
                    .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                    .build(),
            )
        val avroRecordSchema1 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroRecordSchema2 =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Test1")
                .namespace("com.amazonaws.services.schemaregistry")
                .fields()
                .requiredInt("test")
                .endRecord()
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .type(avroRecordSchema1)
                .and()
                .type(avroRecordSchema2)
                .endUnion()

        val recordTest = GenericRecordBuilder(avroRecordSchema1).set("test", 12).build()
        avroData.toConnectData(avroSchema, recordTest)
    }

    @Test
    fun testToConnectEnum() {
        // Enums are just converted to strings, original enum is preserved in parameters
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .enumeration("TestEnum")
                .doc("some documentation")
                .symbols("foo", "bar", "baz")
        val builder = SchemaBuilder.string().name("TestEnum")
        builder.parameter(AVRO_ENUM_DOC_PREFIX_PROP + "TestEnum", "some documentation")
        builder.parameter(AVRO_TYPE_ENUM, "TestEnum")
        for (enumSymbol in arrayOf("foo", "bar", "baz")) {
            builder.parameter("$AVRO_TYPE_ENUM.$enumSymbol", enumSymbol)
        }

        assertEquals(
            SchemaAndValue(builder.build(), "bar"),
            avroData.toConnectData(avroSchema, "bar"),
        )
        assertEquals(
            SchemaAndValue(builder.build(), "bar"),
            avroData.toConnectData(avroSchema, GenericData.EnumSymbol(avroSchema, "bar")),
        )
    }

    @Test
    fun testToConnectEnumWithNoDoc() {
        // Enums are just converted to strings, original enum is preserved in parameters
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .enumeration("TestEnum")
                .symbols("foo", "bar", "baz")
        val builder = SchemaBuilder.string().name("TestEnum")
        builder.parameter(AVRO_TYPE_ENUM, "TestEnum")
        for (enumSymbol in arrayOf("foo", "bar", "baz")) {
            builder.parameter("$AVRO_TYPE_ENUM.$enumSymbol", enumSymbol)
        }

        assertEquals(
            SchemaAndValue(builder.build(), "bar"),
            avroData.toConnectData(avroSchema, "bar"),
        )
        assertEquals(
            SchemaAndValue(builder.build(), "bar"),
            avroData.toConnectData(avroSchema, GenericData.EnumSymbol(avroSchema, "bar")),
        )
    }

    @Test
    fun testToConnectOptionalPrimitiveWithConnectMetadata() {
        val schema =
            SchemaBuilder
                .string()
                .doc("doc")
                .defaultValue("foo")
                .name("com.amazonaws.services.schemaregistry.stringtype")
                .version(2)
                .optional()
                .parameter("foo", "bar")
                .parameter("baz", "baz")
                .build()

        val avroStringSchema = org.apache.avro.SchemaBuilder.builder().stringType()
        avroStringSchema.addProp("connect.name", "com.amazonaws.services.schemaregistry.stringtype")
        avroStringSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(2))
        avroStringSchema.addProp("connect.doc", "doc")
        avroStringSchema.addProp("connect.default", "foo")
        val params = JsonNodeFactory.instance.objectNode()
        params.put("foo", "bar")
        params.put("baz", "baz")
        avroStringSchema.addProp("connect.parameters", params)
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .unionOf()
                .type(avroStringSchema)
                .and()
                .nullType()
                .endUnion()

        assertEquals(
            SchemaAndValue(schema, "string"),
            avroData.toConnectData(avroSchema, "string"),
        )
    }

    @Test
    fun testToConnectRecordWithMetadata() {
        // One important difference between record schemas in Avro and Connect is that Avro has some
        // per-field metadata (doc, default value) that Connect holds in parameters(). We set
        // these properties on one of these fields to ensure they are properly converted
        val schema =
            SchemaBuilder
                .struct()
                .name("com.amazonaws.services.schemaregistry.test.TestSchema")
                .version(12)
                .doc("doc")
                .field(
                    "int32",
                    SchemaBuilder
                        .int32()
                        .defaultValue(7)
                        .parameter(AVRO_FIELD_DEFAULT_FLAG_PROP, "true")
                        .build(),
                ).parameter(AVRO_FIELD_DOC_PREFIX_PROP + "int32", "field doc")
                .build()
        val struct =
            Struct(schema)
                .put("int32", 12)

        val avroSchema =
            org.apache.avro.SchemaBuilder
                .record("TestSchema")
                .namespace("com.amazonaws.services.schemaregistry.test")
                .fields()
                .name("int32")
                .doc("field doc")
                .type()
                .intType()
                .intDefault(7)
                .endRecord()
        avroSchema.addProp("connect.name", "com.amazonaws.services.schemaregistry.test.TestSchema")
        avroSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(12))
        avroSchema.addProp("connect.doc", "doc")
        val avroRecord =
            GenericRecordBuilder(avroSchema)
                .set("int32", 12)
                .build()

        assertEquals(
            SchemaAndValue(schema, struct),
            avroData.toConnectData(avroSchema, avroRecord),
        )
    }

    @Test
    fun testToConnectSchemaless() {
        val avroNullRecord = GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).build()
        assertEquals(
            SchemaAndValue(null, null),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroNullRecord),
        )

        val avroIntRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("int", 12)
                .build()
        assertEquals(
            SchemaAndValue(null, 12),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroIntRecord),
        )

        val avroLongRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("long", 12L)
                .build()
        assertEquals(
            SchemaAndValue(null, 12L),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroLongRecord),
        )

        val avroFloatRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("float", 12.2f)
                .build()
        assertEquals(
            SchemaAndValue(null, 12.2f),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroFloatRecord),
        )

        val avroDoubleRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("double", 12.2)
                .build()
        assertEquals(
            SchemaAndValue(null, 12.2),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroDoubleRecord),
        )

        val avroBooleanRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("boolean", true)
                .build()
        assertEquals(
            SchemaAndValue(null, true),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroBooleanRecord),
        )

        val avroStringRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("string", "teststring")
                .build()
        assertEquals(
            SchemaAndValue(null, "teststring"),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroStringRecord),
        )

        val avroArrayRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("array", listOf(avroIntRecord, avroStringRecord, avroNullRecord))
                .build()
        assertEquals(
            SchemaAndValue(null, listOf(12, "teststring", null)),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroArrayRecord),
        )

        val avroMapEntry =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
                .set("key", avroIntRecord)
                .set("value", avroStringRecord)
                .build()
        val avroMapEntryNull =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA_MAP_ELEMENT)
                .set("key", GenericRecordBuilder(AvroData.ANYTHING_SCHEMA).set("int", 13).build())
                .set("value", avroNullRecord)
                .build()
        val avroMapRecord =
            GenericRecordBuilder(AvroData.ANYTHING_SCHEMA)
                .set("map", listOf(avroMapEntry, avroMapEntryNull))
                .build()
        val convertedMap = HashMap<Any?, Any?>()
        convertedMap[12] = "teststring"
        convertedMap[13] = null
        assertEquals(
            SchemaAndValue(null, convertedMap),
            avroData.toConnectData(AvroData.ANYTHING_SCHEMA, avroMapRecord),
        )
    }

    @Test(expected = DataException::class)
    fun testToConnectSchemaMismatchPrimitive() {
        val avroSchema = org.apache.avro.SchemaBuilder.builder().intType()
        avroData.toConnectData(avroSchema, 12L)
    }

    @Test(expected = DataException::class)
    fun testToConnectSchemaMismatchArray() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .array()
                .items()
                .stringType()
        avroData.toConnectData(avroSchema, listOf(1, 2, 3))
    }

    @Test(expected = DataException::class)
    fun testToConnectSchemaMismatchMapMismatchKey() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .map()
                .values()
                .intType()
        avroData.toConnectData(avroSchema, mapOf(12 to 12))
    }

    @Test(expected = DataException::class)
    fun testToConnectSchemaMismatchMapMismatchValue() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .map()
                .values()
                .intType()
        avroData.toConnectData(avroSchema, mapOf("foo" to 12L))
    }

    @Test(expected = DataException::class)
    fun testToConnectSchemaMismatchRecord() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .requiredString("string")
                .endRecord()
        val avroSchemaWrong =
            org.apache.avro.SchemaBuilder
                .builder()
                .record("Record")
                .fields()
                .requiredInt("string")
                .endRecord()
        val avroRecordWrong =
            GenericRecordBuilder(avroSchemaWrong)
                .set("string", 12)
                .build()

        avroData.toConnectData(avroSchema, avroRecordWrong)
    }

    @Test
    fun testCacheSchemaToConnectConversion() {
        val cache: Cache<Schema, org.apache.avro.Schema> =
            Whitebox.getInternalState(avroData, "toConnectSchemaCache")
        assertEquals(0, cache.size())

        avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().booleanType(), true)
        assertEquals(1, cache.size())

        avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().booleanType(), true)
        assertEquals(1, cache.size())

        avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().intType(), 32)
        assertEquals(2, cache.size())

        // Should hit limit of cache
        avroData.toConnectData(org.apache.avro.SchemaBuilder.builder().stringType(), "foo")
        assertEquals(2, cache.size())
    }

    @Test
    fun testAvroWithAndWithoutMetaData() {
        val s1 =
            """
            {  "type": "record",  "name": "ListingStateChangedEventKeyRecord",  "namespace": "com.acme.property",
              "doc": "Listing State Changed Event Key",  "fields": [    {      "name": "listingUuid",
                  "type": {        "type": "string",        "avro.java.string": "String"      }    }  ]}
            """.trimIndent()
        val s2 =
            """
            {  "type": "record",  "name": "ListingStateChangedEventKeyRecord",  "namespace": "com.acme.property",
              "doc": "Another listing State Changed Event Key",  "fields": [    {      "name": "listingUuid",
                  "type": "string"    }  ]}
            """.trimIndent()

        val avroSchema1 = org.apache.avro.Schema.Parser().parse(s1)
        val avroSchema2 = org.apache.avro.Schema.Parser().parse(s2)

        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build()
        val avroData = AvroData(avroDataConfig)
        val schema1 = avroData.toConnectSchema(avroSchema1)
        val schema2 = avroData.toConnectSchema(avroSchema2)
        assertEquals(schema1.parameters(), schema2.parameters())
    }

    @Test
    fun testIntWithConnectDefault() {
        val s =
            """
            {  "type": "record",  "name": "SomeThing",  "namespace": "com.acme.property",  "fields": [    {
                  "name": "f",      "type": {        "type": "int",        "connect.default": 42,
                    "connect.version": 1      }    }  ]}
            """.trimIndent()

        val avroSchema = org.apache.avro.Schema.Parser().parse(s)

        val avroData = AvroData(0)
        val schema = avroData.toConnectSchema(avroSchema)

        assertEquals(42, schema.field("f").schema().defaultValue())
    }

    @Test
    fun testLongWithConnectDefault() {
        val s =
            """
            {  "type": "record",  "name": "SomeThing",  "namespace": "com.acme.property",  "fields": [    {
                  "name": "f",      "type": {        "type": "long",        "connect.default": 42,
                    "connect.version": 1      }    }  ]}
            """.trimIndent()

        val avroSchema = org.apache.avro.Schema.Parser().parse(s)

        val avroData = AvroData(0)
        val schema = avroData.toConnectSchema(avroSchema)

        assertEquals(42L, schema.field("f").schema().defaultValue())
    }

    @Test
    fun testArrayOfRecordWithNullNamespace() {
        val avroSchema =
            org.apache.avro.SchemaBuilder
                .array()
                .items()
                .record("item")
                .fields()
                .name("value")
                .type()
                .intType()
                .noDefault()
                .endRecord()

        avroData.toConnectSchema(avroSchema)
    }

    @Test
    fun testLogicalTypeWithMatchingNameAndVersion() {
        // When we use a logical type, the builder we get sets a version. If a version is also included in the schema
        // we're converting, the conversion should still work as long as the versions match.
        val schema =
            org.apache.avro.Schema.Parser().parse(
                """{"type":"record","name":"Message","namespace":"org.cmatta.kafka.connect.irc","fields":""" +
                    """[{"name":"createdat","type":{"type":"long","connect.doc":"When this message was received.",""" +
                    """"connect.version":1,"connect.name":"org.apache.kafka.connect.data.Timestamp",""" +
                    """"logicalType":"timestamp-millis"}}]}""",
            )
        avroData.toConnectSchema(schema)
    }

    @Test(expected = DataException::class)
    fun testLogicalTypeWithMismatchingName() {
        // When we use a logical type, the builder we get sets a version. If a version is also included in the schema
        // we're converting, a mismatch between the versions should cause an exception.
        val schema =
            org.apache.avro.Schema.Parser().parse(
                """{"type":"record","name":"Message","namespace":"org.cmatta.kafka.connect.irc","fields":""" +
                    """[{"name":"createdat","type":{"type":"long","connect.doc":"When this message was received.",""" +
                    """"connect.version":1,"connect.name":"com.custom.Timestamp",""" +
                    """"logicalType":"timestamp-millis"}}]}""",
            )
        avroData.toConnectSchema(schema)
    }

    @Test(expected = DataException::class)
    fun testLogicalTypeWithMismatchingVersion() {
        // When we use a logical type, the builder we get sets a version. If a version is also included in the schema
        // we're converting, a mismatch between the versions should cause an exception.
        val schema =
            org.apache.avro.Schema.Parser().parse(
                """{"type":"record","name":"Message","namespace":"org.cmatta.kafka.connect.irc","fields":""" +
                    """[{"name":"createdat","type":{"type":"long","connect.doc":"When this message was received.",""" +
                    """"connect.version":2,"connect.name":"org.apache.kafka.connect.data.Timestamp",""" +
                    """"logicalType":"timestamp-millis"}}]}""",
            )
        avroData.toConnectSchema(schema)
    }

    @Test
    fun testCyclicalAvroSchema() {
        // This test would test the round trip and asserting the intermediate connect data as well

        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, false)
                .build()
        val listAvroData = AvroData(avroDataConfig)
        val linkedListAvroSchema =
            """{"type": "record","name": "linked_list","fields" : """ +
                """[{"name": "value", "type": "long"},""" +
                """{"name": "next", "type": ["null", "linked_list"],"default" : null}]}"""
        val avroParser = org.apache.avro.Schema.Parser()
        val avroSchema = avroParser.parse(linkedListAvroSchema)

        val next =
            GenericRecordBuilder(avroSchema)
                .set("value", 2L)
                .set("next", null)
                .build()

        val headNode =
            GenericRecordBuilder(avroSchema)
                .set("value", 3L)
                .set("next", next)
                .build()

        val schemaAndValue = listAvroData.toConnectData(avroSchema, headNode)!!

        assertNonNullSchemaValue(schemaAndValue)
        var linkedListNode = schemaAndValue.value() as Struct
        assertEquals(3L, linkedListNode.get("value"))
        assertNotNull(linkedListNode.get("next"))
        linkedListNode = linkedListNode.get("next") as Struct
        assertEquals(2L, linkedListNode.get("value"))
        assertNull(linkedListNode.get("next"))

        val genericRecord =
            listAvroData.fromConnectData(
                schemaAndValue.schema(),
                schemaAndValue.value(),
            ) as GenericRecord

        assertEquals(headNode, genericRecord)
    }

    @Test
    fun testArrayCycle() {
        // This test would test the round trip and asserting the intermediate connect data as well
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, false)
                .build()
        val graphAvroData = AvroData(avroDataConfig)
        val avroParser = org.apache.avro.Schema.Parser()

        val graphAvroSchema =
            """{"type": "record","name": "Users","fields" : [{"name": """ +
                """"name", "type": "string"},{"name": "friends", "type" : [ "null", """ +
                """{"type": "array", "items":"Users"}], "default" : null}]}"""

        val graphSchema = avroParser.parse(graphAvroSchema)
        val friendsListSchema =
            graphSchema
                .getField("friends")
                .schema()
                .types[1]

        val friend1 =
            GenericRecordBuilder(graphSchema)
                .set("name", "Person A")
                .build()
        val friend2 =
            GenericRecordBuilder(graphSchema)
                .set("name", "Person B")
                .set("friends", GenericData.Array<Any>(friendsListSchema, listOf(friend1)))
                .build()

        val person =
            GenericRecordBuilder(graphSchema)
                .set("name", "Person C")
                .set("friends", GenericData.Array<Any>(friendsListSchema, listOf(friend1, friend2)))
                .build()

        val schemaAndValue = graphAvroData.toConnectData(graphSchema, person)!!

        val expectedMap =
            mapOf(
                "Person C" to listOf("Person A", "Person B"),
                "Person B" to listOf("Person A"),
                "Person A" to emptyList(),
            )
        assertNonNullSchemaValue(schemaAndValue)
        assertPersons("Person C", schemaAndValue.value(), expectedMap)

        val genericRecord =
            graphAvroData.fromConnectData(
                schemaAndValue.schema(),
                schemaAndValue.value(),
            ) as GenericRecord

        assertEquals(person, genericRecord)
    }

    private fun assertPersons(
        currentPerson: String,
        value: Any?,
        expectedMap: Map<String, List<String>>,
    ) {
        assertNotNull(value)
        assertTrue(value is Struct)
        val personStruct = value as Struct
        assertEquals(currentPerson, personStruct.get("name"))
        if (expectedMap.containsKey(currentPerson) && expectedMap[currentPerson]!!.isNotEmpty()) {
            assertNotNull(personStruct.get("friends"))
            assertTrue(personStruct.get("friends") is List<*>)
            val friends = personStruct.getArray<Any>("friends")
            assertEquals(expectedMap[currentPerson]!!.size, friends.size)
            for (i in friends.indices) {
                assertPersons(expectedMap[currentPerson]!![i], friends[i], expectedMap)
            }
        } else {
            assertNull(personStruct.get("friends"))
        }
    }

    @Test
    fun testMapCycle() {
        // This test would test the round trip and asserting the intermediate connect data as well
        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build()
        val avroData = AvroData(avroDataConfig)
        val avroParser = org.apache.avro.Schema.Parser()

        val mapCycleSchema =
            """{"type": "record","name": "Node","fields" : [{"name": """ +
                """"value", "type": "long"},{"name": "siblings", "type" : [ "null", """ +
                """{"type": "map", "values":"Node"}], "default" : null}]}"""

        val graphSchema = avroParser.parse(mapCycleSchema)
        val node1 =
            GenericRecordBuilder(graphSchema)
                .set("value", 1L)
                .build()
        val node2 =
            GenericRecordBuilder(graphSchema)
                .set("value", 2L)
                .set("siblings", mapOf("node1" to node1))
                .build()

        val siblings = mapOf("node1" to node1, "node2" to node2)
        val person =
            GenericRecordBuilder(graphSchema)
                .set("value", 3L)
                .set("siblings", siblings)
                .build()

        val schemaAndValue = avroData.toConnectData(graphSchema, person)!!
        val expectedMap =
            mapOf(
                3L to mapOf("node1" to 1L, "node2" to 2L),
                2L to mapOf("node1" to 1L),
                1L to emptyMap(),
            )
        assertNonNullSchemaValue(schemaAndValue)
        assertMapCycle(3L, schemaAndValue.value(), expectedMap)
        val genericRecord =
            avroData.fromConnectData(
                schemaAndValue.schema(),
                schemaAndValue.value(),
            ) as GenericRecord

        assertEquals(person, genericRecord)
    }

    private fun assertMapCycle(
        current: Long,
        value: Any?,
        expectedMap: Map<Long, Map<String, Long>>,
    ) {
        assertNotNull(value)
        assertTrue(value is Struct)
        val struct = value as Struct
        assertEquals(current, struct.get("value"))
        if (expectedMap.containsKey(current) && expectedMap[current]!!.isNotEmpty()) {
            assertNotNull(struct.get("siblings"))
            assertTrue(struct.get("siblings") is Map<*, *>)
            val siblings = struct.getMap<String, Any>("siblings")
            assertEquals(expectedMap[current]!!.size, siblings.size)
            assertTrue(expectedMap[current]!!.keys == siblings.keys)
            for (entry in expectedMap[current]!!.entries) {
                assertMapCycle(entry.value, siblings[entry.key], expectedMap)
            }
        } else {
            assertNull(struct.get("siblings"))
        }
    }

    private fun checkNonRecordConversion(
        expectedSchema: org.apache.avro.Schema,
        expected: Any?,
        schema: Schema?,
        value: Any?,
        avroData: AvroData,
    ): NonRecordContainer {
        val converted = avroData.fromConnectData(schema, value)
        assertTrue(converted is NonRecordContainer)
        val container = converted as NonRecordContainer
        assertEquals(expectedSchema, container.getSchema())
        assertEquals(expected, container.value)
        return container
    }

    private fun checkNonRecordConversionNew(
        expectedSchema: org.apache.avro.Schema,
        expected: Any?,
        schema: Schema?,
        value: Any?,
        avroData: AvroData,
    ): NonRecordContainer {
        val converted = avroData.fromConnectData(schema, value)
        assertTrue(converted is NonRecordContainer)
        val container = converted as NonRecordContainer
        assertSchemaEquals(expectedSchema, container.getSchema())
        assertValueEquals(expected, container.value)
        return container
    }

    private fun checkNonRecordConversionNull(schema: Schema?) {
        val converted = avroData.fromConnectData(schema, null)
        assertNull(converted)
    }

    private fun assertNonNullSchemaValue(schemaAndValue: SchemaAndValue) {
        assertNotNull(schemaAndValue)
        assertNotNull(schemaAndValue.schema())
        assertNotNull(schemaAndValue.value())
    }

    private fun assertSchemaEquals(
        expected: org.apache.avro.Schema,
        actual: org.apache.avro.Schema,
    ) {
        assertEquals(expected.objectProps, actual.objectProps)
        assertEquals(expected.logicalType, actual.logicalType)
        assertEquals(expected.type, actual.type)
        assertEquals(expected.doc, actual.doc)
        // added to test new way of handling logical type
        assertEquals(expected.logicalType, actual.logicalType)
        when (actual.type) {
            org.apache.avro.Schema.Type.UNION -> {
                assertEquals(expected.types, actual.types)
            }

            org.apache.avro.Schema.Type.ENUM -> {
                assertEquals(expected.enumSymbols, actual.enumSymbols)
                for (symbol in actual.enumSymbols) {
                    assertEquals(expected.getEnumOrdinal(symbol), actual.getEnumOrdinal(symbol))
                }
                assertEquals(expected.name, actual.name)
                assertEquals(expected.namespace, actual.namespace)
                assertEquals(expected.fullName, actual.fullName)
                assertEquals(expected.aliases, actual.aliases)
            }

            org.apache.avro.Schema.Type.RECORD -> {
                assertFieldEquals(expected.fields, actual.fields)
                assertEquals(expected.isError, actual.isError)
                assertEquals(expected.name, actual.name)
                assertEquals(expected.namespace, actual.namespace)
                assertEquals(expected.fullName, actual.fullName)
                assertEquals(expected.aliases, actual.aliases)
            }

            org.apache.avro.Schema.Type.FIXED -> {
                assertEquals(expected.fixedSize, actual.fixedSize)
                assertEquals(expected.name, actual.name)
                assertEquals(expected.namespace, actual.namespace)
                assertEquals(expected.fullName, actual.fullName)
                assertEquals(expected.aliases, actual.aliases)
            }

            org.apache.avro.Schema.Type.ARRAY -> {
                assertEquals(expected.elementType, actual.elementType)
            }

            else -> {}
        }
    }

    private fun assertFieldEquals(
        expected: List<org.apache.avro.Schema.Field>,
        actual: List<org.apache.avro.Schema.Field>,
    ) {
        val expectedNames = expected.map { it.name() }.toSet()
        val actualNames = actual.map { it.name() }.toSet()
        assertEquals(expectedNames, actualNames)
        for (i in 0 until actualNames.size) {
            assertFieldEquals(expected[i], actual[i])
        }
    }

    private fun assertFieldEquals(
        expected: org.apache.avro.Schema.Field,
        actual: org.apache.avro.Schema.Field,
    ) {
        assertEquals(expected.name(), actual.name())
        assertEquals(expected.aliases(), actual.aliases())
        assertEquals(expected.doc(), actual.doc())
        assertSchemaEquals(expected.schema(), actual.schema())
        val expectedDef = expected.defaultVal()
        val actualDef = actual.defaultVal()
        val msg = "Mismatched default value for field '" + expected.name() + "'"
        if (expectedDef == null) {
            assertNull(msg, actualDef)
            return
        }
        when (actual.schema().type) {
            org.apache.avro.Schema.Type.INT, org.apache.avro.Schema.Type.LONG -> {
                val expectedLong = (expectedDef as Number).toLong()
                val actualLong = (actualDef as Number).toLong()
                assertEquals(msg, expectedLong, actualLong)
            }

            org.apache.avro.Schema.Type.FLOAT, org.apache.avro.Schema.Type.DOUBLE -> {
                val expectedDouble = (expectedDef as Number).toDouble()
                val actualDouble = (actualDef as Number).toDouble()
                assertEquals(msg, expectedDouble, actualDouble, expectedDouble / 100.0)
            }

            org.apache.avro.Schema.Type.BYTES -> {
                assertArrayEquals(msg, expectedDef as ByteArray, actualDef as ByteArray)
            }

            else -> {
                assertEquals(msg, expectedDef, actualDef)
            }
        }
    }

    private fun assertValueEquals(
        expected: Any?,
        actual: Any?,
    ) {
        val actualValue = if (actual is ByteArray) ByteBuffer.wrap(actual) else actual
        val expectedValue = if (expected is ByteArray) ByteBuffer.wrap(expected) else expected
        assertEquals(expectedValue, actualValue)
    }

    private fun parameters(
        key1: String,
        v1: String,
    ): JsonNode = parametersFromConnect(mapOf(key1 to v1))

    private fun parametersFromConnect(params: Map<String, String>): JsonNode {
        val result = JsonNodeFactory.instance.objectNode()
        for (entry in params.entries) {
            result.put(entry.key, entry.value)
        }
        return result
    }

    @Test
    fun testUnionCycle() {
        val schemaStr =
            """
            {
              "type": "record",
              "name": "Person",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "follows",
                  "type": [
                    "null",
                    "string",
                    "Person"
                  ]
                }
              ]
            }
            """.trimIndent()

        val avroDataConfig =
            AvroDataConfig
                .Builder()
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build()
        val graphAvroData = AvroData(avroDataConfig)

        // Use the generated Connect schema
        val schema =
            graphAvroData.fromConnectSchema(
                graphAvroData.toConnectSchema(org.apache.avro.Schema.Parser().parse(schemaStr)),
            )

        val version = 1

        val person = getUnionCycleRecord(schema)
        val sv = graphAvroData.toConnectData(schema, person, version)!!

        assertEquals(sv, graphAvroData.toConnectData(schema, getUnionCycleRecord(schema), version))
        assertEquals(person, graphAvroData.fromConnectData(sv.schema(), sv.value()))
    }

    private fun getUnionCycleRecord(connectSchema: org.apache.avro.Schema): GenericRecord {
        val leader =
            GenericRecordBuilder(connectSchema)
                .set("name", "Leader")
                .set("follows", null)
                .build()
        return GenericRecordBuilder(connectSchema)
            .set("name", "Follower")
            .set("follows", leader)
            .build()
    }

    companion object {
        private const val TEST_SCALE = 2
        private val TEST_DECIMAL = BigDecimal(BigInteger("156"), TEST_SCALE)
        private val TEST_DECIMAL_BYTES = byteArrayOf(0, -100)

        private val EPOCH: GregorianCalendar
        private val EPOCH_PLUS_TEN_THOUSAND_DAYS: GregorianCalendar
        private val EPOCH_PLUS_TEN_THOUSAND_MILLIS: GregorianCalendar

        init {
            EPOCH = GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0)
            EPOCH.timeZone = TimeZone.getTimeZone("UTC")

            EPOCH_PLUS_TEN_THOUSAND_DAYS = GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0)
            EPOCH_PLUS_TEN_THOUSAND_DAYS.timeZone = TimeZone.getTimeZone("UTC")
            EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000)

            EPOCH_PLUS_TEN_THOUSAND_MILLIS = GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0)
            EPOCH_PLUS_TEN_THOUSAND_MILLIS.timeZone = TimeZone.getTimeZone("UTC")
            EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000)
        }

        private val NAMED_MAP_SCHEMA: Schema =
            SchemaBuilder
                .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA)
                .name("foo.bar")
                .build()

        private val NAMED_AVRO_MAP_SCHEMA: org.apache.avro.Schema =
            org.apache.avro.SchemaBuilder
                .array()
                .prop(CONNECT_NAME_PROP, "foo.bar")
                .items(
                    org.apache.avro.SchemaBuilder
                        .record("foo.bar")
                        .prop(CONNECT_INTERNAL_TYPE_NAME, MAP_ENTRY_TYPE_NAME)
                        .fields()
                        .optionalString(KEY_FIELD)
                        .requiredInt(VALUE_FIELD)
                        .endRecord(),
                )

        private fun createDecimalSchema(
            required: Boolean,
            precision: Int,
        ): org.apache.avro.Schema = createDecimalSchema(required, precision, TEST_SCALE)

        private fun createDecimalSchema(
            required: Boolean,
            precision: Int,
            scale: Int,
        ): org.apache.avro.Schema {
            val avroSchema =
                if (required) {
                    org.apache.avro.SchemaBuilder.builder().bytesType()
                } else {
                    org.apache.avro.SchemaBuilder
                        .builder()
                        .unionOf()
                        .nullType()
                        .and()
                        .bytesType()
                        .endUnion()
                }
            val decimalSchema = if (required) avroSchema else avroSchema.types[1]
            decimalSchema.addProp("scale", scale)
            decimalSchema.addProp("precision", precision)
            decimalSchema.addProp("connect.version", JsonNodeFactory.instance.numberNode(1))
            val avroParams = JsonNodeFactory.instance.objectNode()
            avroParams.put("scale", scale.toString())
            avroParams.put(AvroData.CONNECT_AVRO_DECIMAL_PRECISION_PROP, precision.toString())
            decimalSchema.addProp("connect.parameters", avroParams)
            decimalSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal")
            decimalSchema.addProp(AvroData.AVRO_LOGICAL_TYPE_PROP, AvroData.AVRO_LOGICAL_DECIMAL)
            if (scale in 0..precision) {
                LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema)
            }

            return avroSchema
        }
    }
}
