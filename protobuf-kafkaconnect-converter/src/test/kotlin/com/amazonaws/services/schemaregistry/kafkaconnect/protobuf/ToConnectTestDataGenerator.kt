/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf

import additionalTypes.Decimals
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.CommonTestHelper.createConnectSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_PACKAGE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.AllTypesSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.ArrayTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.DecimalTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.EnumTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.MapTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.NestedOneofTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.NestedTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.OneofTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.PrimitiveTypesSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.RecursiveTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax2.TimeTypeSyntax2
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.AllTypesSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.ArrayTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.DecimalTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.EnumTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.MapTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.NestedOneofTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.NestedTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.OneofTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.PrimitiveTypesSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.RecursiveTypeSyntax3
import com.amazonaws.services.schemaregistry.kafkaconnect.tests.syntax3.TimeTypeSyntax3
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import java.math.BigDecimal

/** Each generator returns the syntax3 and syntax2 variants of the same message, in that order. */
object ToConnectTestDataGenerator {
    private fun getFullName(
        packageName: String,
        name: String,
    ): String = listOf(packageName, name).joinToString(".")

    @JvmStatic
    fun getPrimitiveProtobufMessages(): List<Message> = listOf(
        PrimitiveTypesSyntax3.PrimitiveTypes
            .newBuilder()
            .setI8(2)
            .setI8WithParam(0)
            .clearI8Optional()
            .setI8WithDefault(10)
            .setI16(255)
            .setI16WithParam(234)
            .setI16WithDefault(15)
            .setI16Optional(87)
            .setI32(123123)
            .setI32WithParam(23982)
            .setI32WithSameTypeMetadata(2345)
            .setI32WithMetadata(-1829)
            .setI32WithAnotherMetadata(123)
            .setI32WithDefault(0)
            .clearI32Optional()
            .setI64(-23499L)
            .setI64WithParam(7659L)
            .setI64WithDefault(1238102931L)
            .setI64WithSameTypeMetadata(8294L)
            .setI64WithMetadata(9123L)
            .setI64WithAnotherMetadata(8272L)
            .setI64WithYetAnotherMetadata(80123)
            .setI64Optional(91010L)
            .setF32(34.56f)
            .setF32WithParam(89.00f)
            .setF32Optional(81232.1234566f)
            .setF32WithDefault(2456f)
            .setF64(9123.0)
            .setF64WithParam(91202.213)
            .setF64Optional(-927.456)
            .setF64WithDefault(0.0023)
            .setBool(true)
            .setBoolWithParam(true)
            .clearBoolOptional()
            .setBoolWithDefault(false)
            .setBytes(ByteString.copyFrom(byteArrayOf(1, 5, 6, 7)))
            .setBytesWithParam(ByteString.copyFrom(byteArrayOf(1)))
            .clearBytesOptional()
            .setBytesWithDefault(ByteString.copyFrom(byteArrayOf(1, 4, 5, 6)))
            .setStr("asdsai131")
            .setStrWithParam("12351")
            .clearStrOptional()
            .setStrWithDefault("")
            .build(),
        PrimitiveTypesSyntax2.PrimitiveTypes
            .newBuilder()
            .setI8(2)
            .setI8WithParam(0)
            .clearI8Optional()
            .setI8WithDefault(10)
            .setI16(255)
            .setI16WithParam(234)
            .setI16WithDefault(15)
            .setI16Optional(87)
            .setI32(123123)
            .setI32WithParam(23982)
            .setI32WithSameTypeMetadata(2345)
            .setI32WithMetadata(-1829)
            .setI32WithAnotherMetadata(123)
            .setI32WithDefault(0)
            .clearI32Optional()
            .setI64(-23499L)
            .setI64WithParam(7659L)
            .setI64WithDefault(1238102931L)
            .setI64WithSameTypeMetadata(8294L)
            .setI64WithMetadata(9123L)
            .setI64WithAnotherMetadata(8272L)
            .setI64WithYetAnotherMetadata(80123)
            .setI64Optional(91010L)
            .setF32(34.56f)
            .setF32WithParam(89.00f)
            .setF32Optional(81232.1234566f)
            .setF32WithDefault(2456f)
            .setF64(9123.0)
            .setF64WithParam(91202.213)
            .setF64Optional(-927.456)
            .setF64WithDefault(0.0023)
            .setBool(true)
            .setBoolWithParam(true)
            .clearBoolOptional()
            .setBoolWithDefault(false)
            .setBytes(ByteString.copyFrom(byteArrayOf(1, 5, 6, 7)))
            .setBytesWithParam(ByteString.copyFrom(byteArrayOf(1)))
            .clearBytesOptional()
            .setBytesWithDefault(ByteString.copyFrom(byteArrayOf(1, 4, 5, 6)))
            .setStr("asdsai131")
            .setStrWithParam("12351")
            .clearStrOptional()
            .setStrWithDefault("")
            .build(),
    )

    @JvmStatic
    fun getPrimitiveSchema(packageName: String): Schema = createConnectSchema("PrimitiveTypes", getPrimitiveTypes(), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getPrimitiveTypesData(packageName: String): Struct {
        val connectData = Struct(getPrimitiveSchema(packageName))

        connectData
            .put("i8", 2.toByte())
            .put("i8WithParam", 0.toByte())
            .put("i8WithDefault", 10.toByte())
            .put("i16", 255.toShort())
            .put("i16WithParam", 234.toShort())
            .put("i16WithDefault", 15.toShort())
            .put("i16Optional", 87.toShort())
            .put("i32", 123123)
            .put("i32WithParam", 23982)
            .put("i32WithSameTypeMetadata", 2345)
            .put("i32WithMetadata", -1829)
            .put("i32WithAnotherMetadata", 123)
            .put("i32WithDefault", 0)
            .put("i32Optional", null)
            .put("i64", -23499L)
            .put("i64WithParam", 7659L)
            .put("i64WithDefault", 1238102931L)
            .put("i64WithSameTypeMetadata", 8294L)
            .put("i64WithMetadata", 9123L)
            .put("i64WithAnotherMetadata", 8272L)
            .put("i64WithYetAnotherMetadata", 80123L)
            .put("i64Optional", 91010L)
            .put("f32", 34.56f)
            .put("f32WithParam", 89.00f)
            .put("f32Optional", 81232.1234566f)
            .put("f32WithDefault", 2456f)
            .put("f64", 9123.0)
            .put("f64WithParam", 91202.213)
            .put("f64Optional", -927.456)
            .put("f64WithDefault", 0.0023)
            .put("bool", true)
            .put("boolWithParam", true)
            .put("boolOptional", null)
            .put("boolWithDefault", false)
            .put("bytes", byteArrayOf(1, 5, 6, 7))
            .put("bytesWithParam", byteArrayOf(1))
            .put("bytesOptional", null)
            .put("bytesWithDefault", byteArrayOf(1, 4, 5, 6))
            .put("str", "asdsai131")
            .put("strWithParam", "12351")
            .put("strOptional", null)
            .put("strWithDefault", "")
        return connectData
    }

    private fun getPrimitiveTypes(): Map<String, Schema> = linkedMapOf(
        "i8" to SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "1").build(),
        "i8WithParam" to SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "2000").build(),
        "i8Optional" to SchemaBuilder(Schema.Type.INT8).optional().parameter(PROTOBUF_TAG, "2").build(),
        "i8WithDefault" to SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "3").build(),
        "i16" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "4").build(),
        "i16WithParam" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "4123").build(),
        "i16WithDefault" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "5").build(),
        "i16Optional" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "6").optional().build(),
        "i32" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "7").build(),
        "i32WithParam" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "8123").build(),
        "i32WithSameTypeMetadata" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "8").build(),
        "i32WithMetadata" to
            SchemaBuilder(Schema.Type.INT32)
                .parameter(PROTOBUF_TAG, "9")
                .parameter(PROTOBUF_TYPE, "SINT32")
                .build(),
        "i32WithAnotherMetadata" to
            SchemaBuilder(Schema.Type.INT32)
                .parameter(PROTOBUF_TAG, "10")
                .parameter(PROTOBUF_TYPE, "SFIXED32")
                .build(),
        "i32WithDefault" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "11").build(),
        "i32Optional" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "12").optional().build(),
        "i64" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "13").build(),
        "i64WithParam" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "9123").build(),
        "i64WithDefault" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "14").build(),
        "i64WithSameTypeMetadata" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "15").build(),
        "i64WithMetadata" to
            SchemaBuilder(Schema.Type.INT64)
                .parameter(PROTOBUF_TAG, "16")
                .parameter(PROTOBUF_TYPE, "SINT64")
                .build(),
        "i64WithAnotherMetadata" to
            SchemaBuilder(Schema.Type.INT64)
                .parameter(PROTOBUF_TAG, "17")
                .parameter(PROTOBUF_TYPE, "FIXED64")
                .build(),
        "i64WithYetAnotherMetadata" to
            SchemaBuilder(Schema.Type.INT64)
                .parameter(PROTOBUF_TAG, "18")
                .parameter(PROTOBUF_TYPE, "UINT32")
                .build(),
        "i64Optional" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "19").optional().build(),
        "f32" to SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "20").build(),
        "f32WithParam" to SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "10923").build(),
        "f32Optional" to SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "21").optional().build(),
        "f32WithDefault" to SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "22").build(),
        "f64" to SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "23").build(),
        "f64WithParam" to SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "1112").build(),
        "f64Optional" to SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "24").optional().build(),
        "f64WithDefault" to SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "25").build(),
        "bool" to SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "26").build(),
        "boolWithParam" to SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "12123").build(),
        "boolOptional" to SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "27").optional().build(),
        "boolWithDefault" to SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "28").build(),
        "bytes" to SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "29").build(),
        "bytesWithParam" to SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "18924").build(),
        "bytesOptional" to SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "30").optional().build(),
        "bytesWithDefault" to SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "31").build(),
        "str" to SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "32").build(),
        "strWithParam" to SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "13912").build(),
        "strOptional" to SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "33").optional().build(),
        "strWithDefault" to SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "34").build(),
    )

    @JvmStatic
    fun getEnumProtobufMessages(): List<Message> = listOf(
        EnumTypeSyntax3.EnumTest
            .newBuilder()
            .setCorpus(EnumTypeSyntax3.EnumTest.Corpus.UNIVERSAL)
            .setShapes(EnumTypeSyntax3.EnumTest.ShapesWithParam.TRIANGLE)
            .setColor(EnumTypeSyntax3.EnumTest.Colors.BLUE)
            .setFruits(EnumTypeSyntax3.EnumTest.FruitsWithDefault.BANANA)
            .build(),
        EnumTypeSyntax2.EnumTest
            .newBuilder()
            .setCorpus(EnumTypeSyntax2.EnumTest.Corpus.UNIVERSAL)
            .setShapes(EnumTypeSyntax2.EnumTest.ShapesWithParam.TRIANGLE)
            .setColor(EnumTypeSyntax2.EnumTest.Colors.BLUE)
            .setFruits(EnumTypeSyntax2.EnumTest.FruitsWithDefault.BANANA)
            .build(),
    )

    @JvmStatic
    fun getEnumSchema(packageName: String): Schema = createConnectSchema("EnumTest", getEnumType(packageName), mapOf("protobuf.package" to packageName))

    @JvmStatic
    fun getEnumTypeData(packageName: String): Struct {
        val connectData = Struct(getEnumSchema(packageName))

        connectData
            .put("corpus", "UNIVERSAL")
            .put("shapes", "TRIANGLE")
            .put("color", "BLUE")
            .put("fruits", "BANANA")
        return connectData
    }

    private fun getEnumType(packageName: String): Map<String, Schema> = linkedMapOf(
        "corpus" to
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.UNIVERSAL", "0")
                .parameter("PROTOBUF_ENUM_VALUE.WEB", "1")
                .parameter("PROTOBUF_ENUM_VALUE.NEWS", "4")
                .parameter("PROTOBUF_ENUM_VALUE.IMAGES", "2")
                .parameter("PROTOBUF_ENUM_VALUE.LOCAL", "3")
                .parameter("PROTOBUF_ENUM_VALUE.PRODUCTS", "5")
                .parameter("PROTOBUF_ENUM_VALUE.VIDEO", "6")
                .parameter("ENUM_NAME", getFullName(packageName, "EnumTest.Corpus"))
                .parameter("protobuf.tag", "1")
                .build(),
        "shapes" to
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.SQUARE", "0")
                .parameter("PROTOBUF_ENUM_VALUE.CIRCLE", "1")
                .parameter("PROTOBUF_ENUM_VALUE.TRIANGLE", "2")
                .parameter("ENUM_NAME", getFullName(packageName, "EnumTest.ShapesWithParam"))
                .parameter("protobuf.tag", "12345")
                .build(),
        "color" to
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.BLACK", "0")
                .parameter("PROTOBUF_ENUM_VALUE.RED", "1")
                .parameter("PROTOBUF_ENUM_VALUE.GREEN", "2")
                .parameter("PROTOBUF_ENUM_VALUE.BLUE", "3")
                .parameter("ENUM_NAME", getFullName(packageName, "EnumTest.Colors"))
                .parameter("protobuf.tag", "2")
                .optional()
                .build(),
        "fruits" to
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.APPLE", "0")
                .parameter("PROTOBUF_ENUM_VALUE.ORANGE", "1")
                .parameter("PROTOBUF_ENUM_VALUE.BANANA", "2")
                .parameter("ENUM_NAME", getFullName(packageName, "EnumTest.FruitsWithDefault"))
                .parameter("protobuf.tag", "3")
                .build(),
    )

    @JvmStatic
    fun getTimeProtobufMessages(): List<Message> {
        val dateBuilder = com.google.type.Date.newBuilder()
        dateBuilder.setYear(2022)
        dateBuilder.setMonth(3)
        dateBuilder.setDay(20)

        val todBuilder = com.google.type.TimeOfDay.newBuilder()
        todBuilder.setHours(2)
        todBuilder.setMinutes(2)
        todBuilder.setSeconds(42)

        val timestampBuilder = com.google.protobuf.Timestamp.newBuilder()
        timestampBuilder.setSeconds(1)
        timestampBuilder.setNanos(805000000)
        return listOf(
            TimeTypeSyntax3.TimeTypes
                .newBuilder()
                .setDate(dateBuilder)
                .setTime(todBuilder)
                .setTimestamp(timestampBuilder)
                .build(),
            TimeTypeSyntax2.TimeTypes
                .newBuilder()
                .setDate(dateBuilder)
                .setTime(todBuilder)
                .setTimestamp(timestampBuilder)
                .build(),
        )
    }

    @JvmStatic
    fun getTimeSchema(packageName: String): Schema = createConnectSchema("TimeTypes", getTimeTypes(), mapOf("protobuf.package" to packageName))

    @JvmStatic
    fun getTimeTypeData(packageName: String): Struct {
        val connectData = Struct(getTimeSchema(packageName))
        val dateDefVal = 19071 // equal to 2022/03/20 with reference to the unix epoch
        val timeDefVal = 7362000 // equal to 2 hours 2 minutes 42 seconds in millisecond
        val tsDefVal = 1805L // equal to 1 second 805000000 nanoseconds in millisecond
        val date = Date.toLogical(Date.SCHEMA, dateDefVal)
        val time = Time.toLogical(Time.SCHEMA, timeDefVal)
        val timestamp = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal)

        connectData
            .put("date", date)
            .put("time", time)
            .put("timestamp", timestamp)
        return connectData
    }

    private fun getTimeTypes(): Map<String, Schema> = linkedMapOf(
        "date" to Date.builder().parameter(PROTOBUF_TAG, "1").build(),
        "time" to Time.builder().parameter(PROTOBUF_TAG, "2").build(),
        "timestamp" to Timestamp.builder().parameter(PROTOBUF_TAG, "3").build(),
    )

    @JvmStatic
    fun getDecimalProtobufMessages(): List<Message> {
        val decimalBuilder = Decimals.Decimal.newBuilder()
        decimalBuilder.setUnits(1234)
        decimalBuilder.setFraction(567890000)
        decimalBuilder.setPrecision(9)
        decimalBuilder.setScale(5)

        val decimalLargeScale = Decimals.Decimal.newBuilder()
        decimalLargeScale.setUnits(1234)
        decimalLargeScale.setFraction(567891340)
        decimalLargeScale.setPrecision(12)
        decimalLargeScale.setScale(8)

        val decimalZeroScale = Decimals.Decimal.newBuilder()
        decimalZeroScale.setUnits(1234)
        decimalZeroScale.setFraction(0)
        decimalZeroScale.setPrecision(4)
        decimalZeroScale.setScale(0)

        return listOf(
            DecimalTypeSyntax3.DecimalTypes
                .newBuilder()
                .setDecimal(decimalBuilder)
                .setDecimalLargeScale(decimalLargeScale)
                .setDecimalZeroScale(decimalZeroScale)
                .build(),
            DecimalTypeSyntax2.DecimalTypes
                .newBuilder()
                .setDecimal(decimalBuilder)
                .setDecimalLargeScale(decimalLargeScale)
                .setDecimalZeroScale(decimalZeroScale)
                .build(),
        )
    }

    @JvmStatic
    fun getDecimalSchema(packageName: String): Schema = createConnectSchema("DecimalTypes", getDecimalTypes(), mapOf("protobuf.package" to packageName))

    @JvmStatic
    fun getDecimalTypeData(packageName: String): Struct {
        val connectData = Struct(getDecimalSchema(packageName))
        val decimal = BigDecimal.valueOf(1234.56789)
        val decimalLargeScale = BigDecimal.valueOf(1234.56789134)
        val decimalZeroScale = BigDecimal.valueOf(1234)

        connectData
            .put("decimal", decimal)
            .put("decimalLargeScale", decimalLargeScale)
            .put("decimalZeroScale", decimalZeroScale)
        return connectData
    }

    private fun getDecimalTypes(): Map<String, Schema> = linkedMapOf(
        "decimal" to Decimal.builder(DECIMAL_DEFAULT_SCALE).parameter(PROTOBUF_TAG, "1").build(),
        "decimalLargeScale" to
            Decimal
                .builder(10)
                .parameter(PROTOBUF_TAG, "2")
                .parameter("connect.decimal.scale", "10")
                .build(),
        "decimalZeroScale" to
            Decimal
                .builder(1)
                .parameter(PROTOBUF_TAG, "3")
                .parameter("connect.decimal.scale", "1")
                .build(),
    )

    @JvmStatic
    fun getArrayProtobufMessages(): List<Message> = listOf(
        ArrayTypeSyntax3.ArrayType
            .newBuilder()
            .addAllStr(listOf("foo", "bar", "baz"))
            .addBoolean(true)
            .addBoolean(false)
            .build(),
        ArrayTypeSyntax2.ArrayType
            .newBuilder()
            .addAllStr(listOf("foo", "bar", "baz"))
            .addBoolean(true)
            .addBoolean(false)
            .build(),
    )

    @JvmStatic
    fun getArraySchema(packageName: String): Schema = createConnectSchema("ArrayType", getArrayType(), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getArrayTypeData(packageName: String): Struct {
        val connectData = Struct(getArraySchema(packageName))

        connectData
            .put("str", listOf("foo", "bar", "baz"))
            .put("boolean", listOf(true, false))
            .put("i32", ArrayList<Any>())
        return connectData
    }

    private fun getArrayType(): Map<String, Schema> = linkedMapOf(
        "str" to SchemaBuilder.array(Schema.STRING_SCHEMA).parameter(PROTOBUF_TAG, "1").optional().build(),
        "i32" to SchemaBuilder.array(Schema.INT32_SCHEMA).parameter(PROTOBUF_TAG, "2").optional().build(),
        "boolean" to SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).parameter(PROTOBUF_TAG, "3").optional().build(),
    )

    @JvmStatic
    fun getMapProtobufMessages(): List<Message> {
        val booleanMap = HashMap<String, Boolean>()
        booleanMap["A"] = true
        booleanMap["B"] = false
        return listOf(
            MapTypeSyntax3.MapType
                .newBuilder()
                .putIntMap(2, 22)
                .putAllBoolMap(booleanMap)
                .build(),
            MapTypeSyntax2.MapType
                .newBuilder()
                .putIntMap(2, 22)
                .putAllBoolMap(booleanMap)
                .build(),
        )
    }

    @JvmStatic
    fun getMapSchema(packageName: String): Schema = createConnectSchema("MapType", getMapType(), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getMapTypeData(packageName: String): Struct {
        val connectData = Struct(getMapSchema(packageName))

        connectData
            .put("intMap", mapOf(2 to 22))
            .put("boolMap", linkedMapOf("A" to true, "B" to false))
            .put("strMap", HashMap<Any, Any>())
        return connectData
    }

    private fun getMapType(): Map<String, Schema> = linkedMapOf(
        "intMap" to
            SchemaBuilder
                .map(
                    SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "1").optional().build(),
                    SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "2").optional().build(),
                ).parameter(PROTOBUF_TAG, "1")
                .build(),
        "boolMap" to
            SchemaBuilder
                .map(
                    SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "1").optional().build(),
                    SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "2").optional().build(),
                ).parameter(PROTOBUF_TAG, "2")
                .build(),
        "strMap" to
            SchemaBuilder
                .map(
                    SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "1").optional().build(),
                    SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "2").optional().build(),
                ).parameter(PROTOBUF_TAG, "3")
                .build(),
    )

    @JvmStatic
    fun getStructProtobufMessages(): List<Message> {
        val addressSyntax2 =
            NestedTypeSyntax2.Address.newBuilder().setStreet("8th").setZipcode(98121).build()
        val customerSyntax2 =
            NestedTypeSyntax2.NestedType.Customer.newBuilder().setName("joe").build()
        val nestedTypeSyntax2 =
            NestedTypeSyntax2.NestedType
                .newBuilder()
                .setAddress(addressSyntax2)
                .setCustomer(customerSyntax2)
                .setStatus(NestedTypeSyntax2.Status.VALID)
                .setId(12365)
                .putMapping("hello", true)
                .build()
        val addressSyntax3 =
            NestedTypeSyntax3.Address.newBuilder().setStreet("8th").setZipcode(98121).build()
        val customerSyntax3 =
            NestedTypeSyntax3.NestedType.Customer.newBuilder().setName("joe").build()
        val nestedTypeSyntax3 =
            NestedTypeSyntax3.NestedType
                .newBuilder()
                .setAddress(addressSyntax3)
                .setCustomer(customerSyntax3)
                .setStatus(NestedTypeSyntax3.Status.VALID)
                .setId(12365)
                .putMapping("hello", true)
                .build()
        return listOf(nestedTypeSyntax3, nestedTypeSyntax2)
    }

    @JvmStatic
    fun getStructSchema(packageName: String): Schema = createConnectSchema("NestedType", getStructType(packageName), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getStructTypeData(packageName: String): Struct {
        val connectSchema = getStructSchema(packageName)
        val connectData = Struct(connectSchema)

        connectData
            .put("address", Struct(connectSchema.field("address").schema()).put("street", "8th").put("zipcode", 98121))
            .put("status", "VALID")
            .put("customer", Struct(connectSchema.field("customer").schema()).put("name", "joe"))
            .put("mapping", mapOf("hello" to true))
            .put("id", 12365)
        return connectData
    }

    private fun getStructType(packageName: String): Map<String, Schema> {
        val addressBuilder =
            SchemaBuilder
                .struct()
                .name(getFullName(packageName, "Address"))
                .field("street", SchemaBuilder.string().parameter(PROTOBUF_TAG, "1").build())
                .field("zipcode", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "2").build())
        val statusBuilder =
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.VALID", "0")
                .parameter("PROTOBUF_ENUM_VALUE.INVALID", "1")
                .parameter("ENUM_NAME", getFullName(packageName, "Status"))
        val customerBuilder =
            SchemaBuilder
                .struct()
                .name(getFullName(packageName, "NestedType.Customer"))
                .field("name", SchemaBuilder.string().parameter(PROTOBUF_TAG, "1").build())
        val mappingBuilder =
            SchemaBuilder.map(
                SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "1").optional().build(),
                SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "2").optional().build(),
            )

        return linkedMapOf(
            "address" to addressBuilder.parameter(PROTOBUF_TAG, "1").build(),
            "status" to statusBuilder.parameter(PROTOBUF_TAG, "2").build(),
            "customer" to customerBuilder.parameter(PROTOBUF_TAG, "3").build(),
            "mapping" to mappingBuilder.parameter(PROTOBUF_TAG, "4").build(),
            "id" to SchemaBuilder.int32().parameter(PROTOBUF_TAG, "5").optional().build(),
        )
    }

    @JvmStatic
    fun getOneofProtobufMessages(): List<Message> = listOf(
        OneofTypeSyntax3.OneofType
            .newBuilder()
            .setName("Jeff")
            .setShipped(true)
            .build(),
        OneofTypeSyntax2.OneofType
            .newBuilder()
            .setName("Jeff")
            .setShipped(true)
            .build(),
    )

    @JvmStatic
    fun getOneofSchema(packageName: String): Schema = createConnectSchema("OneofType", getOneofType(), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getOneofTypeData(packageName: String): Struct {
        val connectData = Struct(getOneofSchema(packageName))
        val connectSchema = getOneofSchema(packageName)

        connectData
            .put("customer", Struct(connectSchema.field("customer").schema()).put("name", "Jeff"))
            .put("order", Struct(connectSchema.field("order").schema()).put("shipped", true))
        return connectData
    }

    private fun getOneofType(): Map<String, Schema> = linkedMapOf(
        "customer" to
            SchemaBuilder
                .struct()
                .name("customer")
                .field("name", SchemaBuilder.string().parameter(PROTOBUF_TAG, "5").optional().build())
                .field("age", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "6").optional().build())
                .parameter("protobuf.type", "oneof")
                .optional()
                .build(),
        "order" to
            SchemaBuilder
                .struct()
                .name("order")
                .field("id", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "1").optional().build())
                .field("shipped", SchemaBuilder.bool().parameter(PROTOBUF_TAG, "2").optional().build())
                .parameter("protobuf.type", "oneof")
                .optional()
                .build(),
    )

    @JvmStatic
    fun getNestedOneofProtobufMessages(): List<Message> = listOf(
        NestedOneofTypeSyntax3.NestedOneofType
            .newBuilder()
            .setPayment(
                NestedOneofTypeSyntax3.NestedOneofType.Payment
                    .newBuilder()
                    .setReference("INV-1")
                    .setCard("4111"),
            ).build(),
        NestedOneofTypeSyntax2.NestedOneofType
            .newBuilder()
            .setPayment(
                NestedOneofTypeSyntax2.NestedOneofType.Payment
                    .newBuilder()
                    .setReference("INV-1")
                    .setCard("4111"),
            ).build(),
    )

    @JvmStatic
    fun getNestedOneofSchema(packageName: String): Schema = createConnectSchema("NestedOneofType", getNestedOneofType(packageName), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getNestedOneofTypeData(packageName: String): Struct {
        val connectSchema = getNestedOneofSchema(packageName)
        val paymentSchema = connectSchema.field("payment").schema()
        val payment =
            Struct(paymentSchema)
                .put("reference", "INV-1")
                .put("method", Struct(paymentSchema.field("method").schema()).put("card", "4111"))

        return Struct(connectSchema).put("payment", payment)
    }

    private fun getNestedOneofType(packageName: String): Map<String, Schema> = linkedMapOf(
        "payment" to
            SchemaBuilder
                .struct()
                .name("$packageName.NestedOneofType.Payment")
                .field("reference", SchemaBuilder.string().parameter(PROTOBUF_TAG, "1").build())
                .field(
                    "method",
                    SchemaBuilder
                        .struct()
                        .name("method")
                        .field("card", SchemaBuilder.string().parameter(PROTOBUF_TAG, "2").optional().build())
                        .field("voucher", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "3").optional().build())
                        .parameter("protobuf.type", "oneof")
                        .optional()
                        .build(),
                ).parameter(PROTOBUF_TAG, "1")
                .build(),
    )

    @JvmStatic
    fun getAllTypesProtobufMessages(): List<Message> {
        val dateBuilder = com.google.type.Date.newBuilder()
        dateBuilder.setYear(2022)
        dateBuilder.setMonth(3)
        dateBuilder.setDay(20)
        val todBuilder = com.google.type.TimeOfDay.newBuilder()
        todBuilder.setHours(2)
        todBuilder.setMinutes(2)
        todBuilder.setSeconds(42)
        val timestampBuilder = com.google.protobuf.Timestamp.newBuilder()
        timestampBuilder.setSeconds(1)
        timestampBuilder.setNanos(805000000)

        val booleanMap = HashMap<String, Boolean>()
        booleanMap["A"] = true
        booleanMap["B"] = false

        val addressSyntax2 =
            AllTypesSyntax2.AddressAllTypes.newBuilder().setStreet("8th").setZipcode(98121).build()
        val customerSyntax2 =
            AllTypesSyntax2.AllTypes.Customer.newBuilder().setName("joe").build()

        val decimalBuilder = Decimals.Decimal.newBuilder()
        decimalBuilder.setUnits(1234)
        decimalBuilder.setFraction(567890000)
        decimalBuilder.setPrecision(9)
        decimalBuilder.setScale(5)

        val decimalwithScaleBuilder = Decimals.Decimal.newBuilder()
        decimalwithScaleBuilder.setUnits(1234)
        decimalwithScaleBuilder.setFraction(567891340)
        decimalwithScaleBuilder.setPrecision(12)
        decimalwithScaleBuilder.setScale(8)

        val allTypesSyntax2 =
            AllTypesSyntax2.AllTypes
                .newBuilder()
                .setI32(32)
                .setBool(false)
                .setBytes(ByteString.copyFrom(byteArrayOf(1, 5, 6, 7)))
                .setStr("Hello world!")
                .setI8Optional(2)
                .setI16Optional(255)
                .setI64Optional(1080L)
                .addAllStrArray(listOf("foo", "bar", "baz"))
                .addBoolArray(true)
                .addBoolArray(false)
                .addCustomerArray(customerSyntax2)
                .addColorArray(AllTypesSyntax2.AllTypes.Colors.RED)
                .setDate(dateBuilder)
                .setTime(todBuilder)
                .setTimestamp(timestampBuilder)
                .putIntMap(2, 22)
                .putAllBoolMap(booleanMap)
                .setColor(AllTypesSyntax2.AllTypes.Colors.BLACK)
                .setProgress(AllTypesSyntax2.Progress.INPROGRESS)
                .setId(12315)
                .setAddress(addressSyntax2)
                .setCustomer(customerSyntax2)
                .setDecimal(decimalBuilder)
                .setDecimalWithScale(decimalwithScaleBuilder)
                .build()

        val addressSyntax3 =
            AllTypesSyntax3.AddressAllTypes.newBuilder().setStreet("8th").setZipcode(98121).build()
        val customerSyntax3 =
            AllTypesSyntax3.AllTypes.Customer.newBuilder().setName("joe").build()

        val allTypesSyntax3 =
            AllTypesSyntax3.AllTypes
                .newBuilder()
                .setI32(32)
                .setBool(false)
                .setBytes(ByteString.copyFrom(byteArrayOf(1, 5, 6, 7)))
                .setStr("Hello world!")
                .setI8Optional(2)
                .setI16Optional(255)
                .setI64Optional(1080L)
                .addAllStrArray(listOf("foo", "bar", "baz"))
                .addBoolArray(true)
                .addBoolArray(false)
                .addCustomerArray(customerSyntax3)
                .addColorArray(AllTypesSyntax3.AllTypes.Colors.RED)
                .setDate(dateBuilder)
                .setTime(todBuilder)
                .setTimestamp(timestampBuilder)
                .putIntMap(2, 22)
                .putAllBoolMap(booleanMap)
                .setColor(AllTypesSyntax3.AllTypes.Colors.BLACK)
                .setProgress(AllTypesSyntax3.Progress.INPROGRESS)
                .setId(12315)
                .setAddress(addressSyntax3)
                .setCustomer(customerSyntax3)
                .setDecimal(decimalBuilder)
                .setDecimalWithScale(decimalwithScaleBuilder)
                .build()

        return listOf(allTypesSyntax3, allTypesSyntax2)
    }

    @JvmStatic
    fun getAllTypesSchema(packageName: String): Schema = createConnectSchema("AllTypes", getAllTypes(packageName), mapOf(PROTOBUF_PACKAGE to packageName))

    @JvmStatic
    fun getAllTypesData(packageName: String): Struct {
        val connectSchema = getAllTypesSchema(packageName)
        val connectData = Struct(connectSchema)

        val dateDefVal = 19071 // equal to 2022/03/20 with reference to the unix epoch
        val timeDefVal = 7362000 // equal to 2 hours 2 minutes 42 seconds in millisecond
        val tsDefVal = 1805L // equal to 1 second 805000000 nanoseconds in millisecond
        val date = Date.toLogical(Date.SCHEMA, dateDefVal)
        val time = Time.toLogical(Time.SCHEMA, timeDefVal)
        val timestamp = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal)

        connectData
            .put("i32", 32)
            .put("bool", false)
            .put("bytes", byteArrayOf(1, 5, 6, 7))
            .put("str", "Hello world!")
            .put("i8Optional", 2.toByte())
            .put("i16Optional", 255.toShort())
            .put("i64Optional", 1080L)
            .put("strArray", listOf("foo", "bar", "baz"))
            .put("boolArray", listOf(true, false))
            .put("intArray", ArrayList<Any>())
            .put(
                "customerArray",
                listOf(Struct(connectSchema.field("customerArray").schema().valueSchema()).put("name", "joe")),
            ).put("colorArray", listOf("RED"))
            .put("date", date)
            .put("time", time)
            .put("timestamp", timestamp)
            .put("intMap", mapOf(2 to 22))
            .put("boolMap", linkedMapOf("A" to true, "B" to false))
            .put("strMap", HashMap<Any, Any>())
            .put("color", "BLACK")
            .put("progress", "INPROGRESS")
            .put("order", Struct(connectSchema.field("order").schema()).put("id", 12315))
            .put("address", Struct(connectSchema.field("address").schema()).put("street", "8th").put("zipcode", 98121))
            .put("customer", Struct(connectSchema.field("customer").schema()).put("name", "joe"))
            .put("decimal", BigDecimal.valueOf(1234.56789))
            .put("decimalWithScale", BigDecimal.valueOf(1234.56789134))
        return connectData
    }

    private fun getAllTypes(packageName: String): Map<String, Schema> {
        val addressBuilder =
            SchemaBuilder
                .struct()
                .name(getFullName(packageName, "AddressAllTypes"))
                .field("street", SchemaBuilder.string().parameter(PROTOBUF_TAG, "1").build())
                .field("zipcode", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "2").build())
        val customerBuilder =
            SchemaBuilder
                .struct()
                .name(getFullName(packageName, "AllTypes.Customer"))
                .field("name", SchemaBuilder.string().parameter(PROTOBUF_TAG, "1").build())
        val progressBuilder =
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.INPROGRESS", "0")
                .parameter("PROTOBUF_ENUM_VALUE.REVIEW", "1")
                .parameter("PROTOBUF_ENUM_VALUE.DONE", "2")
                .parameter("ENUM_NAME", getFullName(packageName, "Progress"))
        val colorBuilder =
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.BLACK", "0")
                .parameter("PROTOBUF_ENUM_VALUE.RED", "1")
                .parameter("PROTOBUF_ENUM_VALUE.GREEN", "2")
                .parameter("PROTOBUF_ENUM_VALUE.BLUE", "3")
                .parameter("ENUM_NAME", getFullName(packageName, "AllTypes.Colors"))
        val colorArrayBuilder =
            SchemaBuilder(Schema.Type.STRING)
                .parameter("protobuf.type", "enum")
                .parameter("PROTOBUF_ENUM_VALUE.BLACK", "0")
                .parameter("PROTOBUF_ENUM_VALUE.RED", "1")
                .parameter("PROTOBUF_ENUM_VALUE.GREEN", "2")
                .parameter("PROTOBUF_ENUM_VALUE.BLUE", "3")
                .parameter("ENUM_NAME", getFullName(packageName, "AllTypes.Colors"))

        val intMapBuilder =
            SchemaBuilder.map(
                SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "1").optional().build(),
                SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "2").optional().build(),
            )
        val boolMapBuilder =
            SchemaBuilder.map(
                SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "1").optional().build(),
                SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "2").optional().build(),
            )
        val strMapBuilder =
            SchemaBuilder.map(
                SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "1").optional().build(),
                SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "2").optional().build(),
            )

        return linkedMapOf(
            "i32" to SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "1").build(),
            "bool" to SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "2").optional().build(),
            "bytes" to SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "3").optional().build(),
            "str" to SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "4").optional().build(),
            "i8Optional" to SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "5").optional().build(),
            "i16Optional" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "6").optional().build(),
            "i64Optional" to SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "7").optional().build(),
            "strArray" to SchemaBuilder.array(Schema.STRING_SCHEMA).parameter(PROTOBUF_TAG, "8").optional().build(),
            "intArray" to SchemaBuilder.array(Schema.INT32_SCHEMA).parameter(PROTOBUF_TAG, "9").optional().build(),
            "boolArray" to SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).parameter(PROTOBUF_TAG, "10").optional().build(),
            "customerArray" to
                SchemaBuilder.array(customerBuilder.build()).parameter(PROTOBUF_TAG, "25").optional().build(),
            "colorArray" to
                SchemaBuilder.array(colorArrayBuilder.build()).parameter(PROTOBUF_TAG, "26").optional().build(),
            "date" to Date.builder().parameter(PROTOBUF_TAG, "11").build(),
            "time" to Time.builder().parameter(PROTOBUF_TAG, "12").optional().build(),
            "timestamp" to Timestamp.builder().parameter(PROTOBUF_TAG, "13").optional().build(),
            "intMap" to intMapBuilder.parameter(PROTOBUF_TAG, "14").build(),
            "boolMap" to boolMapBuilder.parameter(PROTOBUF_TAG, "15").build(),
            "strMap" to strMapBuilder.parameter(PROTOBUF_TAG, "16").build(),
            "color" to colorBuilder.parameter(PROTOBUF_TAG, "17").optional().build(),
            "progress" to progressBuilder.parameter(PROTOBUF_TAG, "18").build(),
            "order" to
                SchemaBuilder
                    .struct()
                    .name("order")
                    .field("id", SchemaBuilder.int32().parameter(PROTOBUF_TAG, "19").optional().build())
                    .field("paid", SchemaBuilder.bool().parameter(PROTOBUF_TAG, "20").optional().build())
                    .parameter("protobuf.type", "oneof")
                    .optional()
                    .build(),
            "address" to addressBuilder.parameter(PROTOBUF_TAG, "21").build(),
            "customer" to customerBuilder.parameter(PROTOBUF_TAG, "22").optional().build(),
            "decimal" to Decimal.builder(DECIMAL_DEFAULT_SCALE).parameter(PROTOBUF_TAG, "23").optional().build(),
            "decimalWithScale" to
                Decimal
                    .builder(10)
                    .parameter(PROTOBUF_TAG, "24")
                    .parameter("connect.decimal.scale", "10")
                    .optional()
                    .build(),
        )
    }

    @JvmStatic
    fun getRecursiveProtobufMessages(): List<Message> {
        val childNode1 = RecursiveTypeSyntax2.RecursiveType.newBuilder().setName("child1").build()
        val treeChild1 = RecursiveTypeSyntax2.TreeNode.newBuilder().setValue(1).build()
        val childNode2 = RecursiveTypeSyntax3.RecursiveType.newBuilder().setName("child2").build()
        val treeChild2 = RecursiveTypeSyntax3.TreeNode.newBuilder().setValue(2).build()
        return listOf(
            RecursiveTypeSyntax3.RecursiveType.newBuilder().setName("test").build(),
            RecursiveTypeSyntax2.RecursiveType.newBuilder().setName("test").build(),
            RecursiveTypeSyntax2.RecursiveType.newBuilder().setName("2LevelTest").addChildren(childNode1).build(),
            RecursiveTypeSyntax3.RecursiveType.newBuilder().setName("2LevelTest").addChildren(childNode2).build(),
            RecursiveTypeSyntax3.TreeNode.newBuilder().setValue(42).build(),
            RecursiveTypeSyntax2.TreeNode.newBuilder().setValue(42).build(),
            RecursiveTypeSyntax2.TreeNode.newBuilder().setRight(treeChild1).setValue(4).build(),
            RecursiveTypeSyntax3.TreeNode.newBuilder().setLeft(treeChild2).setValue(3).build(),
        )
    }
}
