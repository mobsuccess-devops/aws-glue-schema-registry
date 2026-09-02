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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.nullOf
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class ConnectDataToProtobufDataConverterTest {
    private val connectDataToProtobufDataConverter = ConnectDataToProtobufDataConverter()

    @Test
    fun convert_ForPrimitiveTypes_ConvertsSuccessfully() {
        val primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()
        val fileDescriptor = primitiveMessage.descriptorForType.file
        val primitiveSchema = ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                primitiveSchema,
                ToProtobufTestDataGenerator.getPrimitiveTypesData(),
            )

        assertEquals(primitiveMessage, actualMessage)
    }

    @Test
    fun convert_ForEnumTypes_ConvertsSuccessfully() {
        val enumMessage = ToProtobufTestDataGenerator.getProtobufEnumMessage("EnumType")
        val fileDescriptor = enumMessage.descriptorForType.file
        val enumSchema = ToProtobufTestDataGenerator.getEnumSchema("EnumType")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                enumSchema,
                ToProtobufTestDataGenerator.getEnumTypeData("EnumType"),
            )

        assertEquals(enumMessage, actualMessage)
    }

    @Test
    fun convert_ForArrayType_ConvertsSuccessfully() {
        val arrayMessage = ToProtobufTestDataGenerator.getProtobufArrayMessage()
        val fileDescriptor = arrayMessage.descriptorForType.file
        val arraySchema = ToProtobufTestDataGenerator.getArraySchema("arrayProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                arraySchema,
                ToProtobufTestDataGenerator.getArrayTypeData(),
            )

        assertEquals(arrayMessage, actualMessage)
    }

    @Test
    fun convert_ForMapType_ConvertsSuccessfully() {
        val mapMessage = ToProtobufTestDataGenerator.getProtobufMapMessage()
        val fileDescriptor = mapMessage.descriptorForType.file
        val mapSchema = ToProtobufTestDataGenerator.getMapSchema("mapProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                mapSchema,
                ToProtobufTestDataGenerator.getMapTypeData(),
            )

        assertEquals(mapMessage, actualMessage)
    }

    @Test
    fun convert_ForTimeType_ConvertsSuccessfully() {
        val timeMessage = ToProtobufTestDataGenerator.getProtobufTimeMessage()
        val fileDescriptor = timeMessage.descriptorForType.file
        val timeSchema = ToProtobufTestDataGenerator.getTimeSchema("timeProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                timeSchema,
                ToProtobufTestDataGenerator.getTimeTypeData(),
            )

        assertEquals(timeMessage, actualMessage)
    }

    @Test
    fun convert_ForDecimalType_ConvertsSuccessfully() {
        val decimalMessage = ToProtobufTestDataGenerator.getProtobufDecimalMessage()
        val fileDescriptor = decimalMessage.descriptorForType.file
        val decimalSchema = ToProtobufTestDataGenerator.getDecimalSchema("decimalProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                decimalSchema,
                ToProtobufTestDataGenerator.getDecimalTypeData(),
            )

        assertEquals(decimalMessage, actualMessage)
    }

    @Test
    fun convert_ForNestedType_ConvertsSuccessfully() {
        val nestedMessage = ToProtobufTestDataGenerator.getProtobufNestedMessage("NestedType")
        val fileDescriptor = nestedMessage.descriptorForType.file
        val nestedSchema = ToProtobufTestDataGenerator.getStructSchema("NestedType")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                nestedSchema,
                ToProtobufTestDataGenerator.getStructTypeData("NestedType"),
            )

        assertEquals(nestedMessage, actualMessage)
    }

    @Test
    fun convert_ForNestedTypeWithoutProtobufMetadata_ConvertsSuccessfully() {
        val nestedSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .field("innerField", Schema.STRING_SCHEMA)
                .build()
        val parentSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("ParentMessage")
                .field("nestedField", nestedSchema)
                .build()
        val fileDescriptor = ConnectSchemaToProtobufSchemaConverter().convert(parentSchema)
        val data =
            Struct(parentSchema)
                .put("nestedField", Struct(nestedSchema).put("innerField", "inner-value"))

        val actualMessage = connectDataToProtobufDataConverter.convert(fileDescriptor, parentSchema, data)

        val nestedMessage =
            actualMessage.getField(actualMessage.descriptorForType.findFieldByName("nestedField")) as Message
        assertEquals(
            "inner-value",
            nestedMessage.getField(nestedMessage.descriptorForType.findFieldByName("innerField")),
        )
    }

    @Test
    fun convert_ForTopLevelSchemaWithoutAName_ThrowsNamedException() {
        val namedSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("NamedParent")
                .field("innerField", Schema.STRING_SCHEMA)
                .build()
        val fileDescriptor = ConnectSchemaToProtobufSchemaConverter().convert(namedSchema)
        val unnamedSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .field("innerField", Schema.STRING_SCHEMA)
                .build()
        val data = Struct(unnamedSchema).put("innerField", "value")

        val exception =
            assertThrows(DataException::class.java) {
                connectDataToProtobufDataConverter.convert(fileDescriptor, unnamedSchema, data)
            }

        assertEquals(
            "No protobuf message type for STRUCT schema <unnamed>. A nested STRUCT is resolved from the field " +
                "descriptor of its parent; a top-level one is resolved by schema name, which requires the schema " +
                "to carry one.",
            exception.message,
        )
    }

    @Test
    fun convert_ForOneofType_ConvertsSuccessfully() {
        val oneofMessage = ToProtobufTestDataGenerator.getProtobufOneofMessage()
        val fileDescriptor = oneofMessage.descriptorForType.file
        val oneofSchema = ToProtobufTestDataGenerator.getOneofSchema("oneofProtobufSchema")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                oneofSchema,
                ToProtobufTestDataGenerator.getOneofTypeData(),
            )

        assertEquals(oneofMessage, actualMessage)
    }

    @Test
    fun convert_ForAllTypes_ConvertsSuccessfully() {
        val message = ToProtobufTestDataGenerator.getProtobufAllTypesMessage("AllTypes")
        val fileDescriptor = message.descriptorForType.file
        val nestedSchema = ToProtobufTestDataGenerator.getAllTypesSchema("AllTypes")
        val actualMessage =
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                nestedSchema,
                ToProtobufTestDataGenerator.getAllTypesData("AllTypes"),
            )

        assertEquals(message, actualMessage)
    }

    @Test
    fun convert_ForNullValues_ThrowsException() {
        val primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()
        val primitiveSchema = ToProtobufTestDataGenerator.getPrimitiveSchema("primitiveProtobufSchema")
        val fileDescriptor = primitiveMessage.descriptorForType.file

        assertThrows(NullPointerException::class.java) {
            connectDataToProtobufDataConverter.convert(
                nullOf(),
                primitiveSchema,
                ToProtobufTestDataGenerator.getPrimitiveTypesData(),
            )
        }

        assertThrows(NullPointerException::class.java) {
            connectDataToProtobufDataConverter.convert(
                fileDescriptor,
                nullOf(),
                ToProtobufTestDataGenerator.getPrimitiveTypesData(),
            )
        }

        assertThrows(NullPointerException::class.java) {
            connectDataToProtobufDataConverter.convert(fileDescriptor, primitiveSchema, nullOf())
        }
    }

    @Test
    fun convert_WhenSchemaIsNotOptionalForNullValues_ThrowsException() {
        val primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()
        val nonOptionalSchema =
            SchemaBuilder
                .struct()
                .name("primitiveProtobufSchema")
                .field("nonOpt", SchemaBuilder.int64())
                .build()
        val nonOptionalField = Field("nonOpt", 0, SchemaBuilder.int64().optional())
        val fileDescriptor = primitiveMessage.descriptorForType.file
        val value = Struct(nonOptionalSchema).put(nonOptionalField, null)

        assertThrows(DataException::class.java) {
            connectDataToProtobufDataConverter.convert(fileDescriptor, nonOptionalSchema, value)
        }
    }

    @Test
    fun convert_WhenValueCannotBeCasted_ThrowsException() {
        val primitiveMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()
        val nonOptionalSchema =
            SchemaBuilder
                .struct()
                .name("primitiveProtobufSchema")
                .field("nonOpt", SchemaBuilder.int32())
                .build()
        val nonOptionalField = Field("nonOpt", 0, SchemaBuilder.string())
        val fileDescriptor = primitiveMessage.descriptorForType.file
        val value = Struct(nonOptionalSchema).put(nonOptionalField, "some-string")

        assertThrows(DataException::class.java) {
            connectDataToProtobufDataConverter.convert(fileDescriptor, nonOptionalSchema, value)
        }
    }

    @ParameterizedTest
    @MethodSource("getInvalidSchemaTypesForConverters")
    fun convert_ThrowsException_WhenIncorrectSchemaTypeIsSentToConverter(
        dataConverter: DataConverter,
        schema: Schema,
    ) {
        val anyMessage = ToProtobufTestDataGenerator.getProtobufPrimitiveMessage()
        val anyFieldDescriptor: Descriptors.FieldDescriptor = anyMessage.descriptorForType.fields[0]
        val fileDescriptor = anyMessage.descriptorForType.file
        assertThrows(DataException::class.java) {
            dataConverter.toProtobufData(fileDescriptor, schema, anyMessage, anyFieldDescriptor, anyMessage.toBuilder())
        }
    }

    companion object {
        @JvmStatic
        fun getInvalidSchemaTypesForConverters(): Stream<Arguments> = Stream.of(
            Arguments.of(
                PrimitiveDataConverter(),
                SchemaBuilder.struct().build(),
            ),
        )
    }
}
