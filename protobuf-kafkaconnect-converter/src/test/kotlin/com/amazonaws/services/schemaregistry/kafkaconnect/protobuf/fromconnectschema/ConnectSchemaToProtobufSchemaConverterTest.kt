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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getAllTypesSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getArraySchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getDecimalSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getEnumSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getMapSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getOneofSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getStructSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getTimeSchema
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata.ConnectDataToProtobufDataConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.nullOf
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata.ProtobufDataToConnectDataConverter
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema.ProtobufSchemaToConnectSchemaConverter
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
import org.junit.jupiter.params.provider.ValueSource
import java.util.stream.Stream

class ConnectSchemaToProtobufSchemaConverterTest {
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaTestCases")
    fun fromConnectSchema_convertsConnectSchemaToProtobufSchema(
        fileName: String,
        connectSchema: Schema,
        expectedProtobufSchema: String,
    ) {
        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)

        val actualSchema = protobufSchema.toProto().toString()

        assertEquals(expectedProtobufSchema, actualSchema)
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaExceptionTestCases")
    fun fromConnectSchema_convertsConnectSchemaToProtobufSchema_forExceptions(
        fileName: String,
        connectTypes: Map<String, Schema>,
    ) {
        val parentSchemaBuilder = SchemaBuilder(Schema.Type.STRUCT)
        parentSchemaBuilder.name(fileName)

        connectTypes
            .forEach { (name, schema) -> parentSchemaBuilder.field(name, schema) }

        val connectSchema = parentSchemaBuilder.build()

        assertThrows(DataException::class.java) {
            CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)
        }
    }

    @Test
    fun fromConnectSchema_onNullSchema_ThrowsException() {
        assertThrows(NullPointerException::class.java) {
            CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(nullOf())
        }
    }

    @Test
    fun fromConnectSchema_structWithNullParametersAndName_doesNotThrowNPE() {
        val nestedStruct =
            SchemaBuilder(Schema.Type.STRUCT)
                .field("innerField", Schema.STRING_SCHEMA)
                .build()
        val parentSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("ParentMessage")
                .field("nestedField", nestedStruct)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(parentSchema)

        assertEquals(1, protobufSchema.messageTypes.size)
        val parentMessage = protobufSchema.messageTypes[0]
        assertEquals("NestedField", parentMessage.nestedTypes[0].name)
        assertEquals(parentMessage.nestedTypes[0], parentMessage.findFieldByName("nestedField").messageType)
    }

    @Test
    fun fromConnectSchema_unnamedStructNestedTwoLevelsDeep_resolvesItsTypeName() {
        val innerStruct =
            SchemaBuilder(Schema.Type.STRUCT)
                .field("innerField", Schema.STRING_SCHEMA)
                .build()
        val nestedStruct =
            SchemaBuilder(Schema.Type.STRUCT)
                .field("innerStruct", innerStruct)
                .build()
        val parentSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("ParentMessage")
                .field("nestedField", nestedStruct)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(parentSchema)

        val nestedField = protobufSchema.messageTypes[0].nestedTypes[0]
        val innerStructType = nestedField.nestedTypes[0]
        assertEquals("InnerStruct", innerStructType.name)
        assertEquals(innerStructType, nestedField.findFieldByName("innerStruct").messageType)
    }

    @Test
    fun fromConnectSchema_dottedDebeziumSchemaName_buildsTheFileDescriptor() {
        val connectSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("inventory.customers.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)

        assertEquals("$AUTOGENERATED_PACKAGE.inventory.customers.Value", protobufSchema.getPackage())
        assertEquals(listOf("Value"), protobufSchema.messageTypes.map { it.name })
        assertEquals(listOf("id", "email"), protobufSchema.messageTypes[0].fields.map { it.name })
    }

    @Test
    fun fromConnectSchema_hyphenatedServerNameInSchemaName_buildsAValidPackage() {
        val connectSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("my-server.inventory.customers.Value")
                .field("id", Schema.INT32_SCHEMA)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)

        assertEquals("$AUTOGENERATED_PACKAGE.my_server.inventory.customers.Value", protobufSchema.getPackage())
        assertEquals("Value", protobufSchema.messageTypes[0].name)
    }

    @Test
    fun fromConnectSchema_fieldNamesThatAreNotIdentifiers_buildsTheFileDescriptor() {
        val connectSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("Envelope")
                .field("ts.ms", Schema.INT64_SCHEMA)
                .field("field-one", Schema.STRING_SCHEMA)
                .field("2nd field", Schema.STRING_SCHEMA)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)

        assertEquals(
            listOf("ts_ms", "field_one", "_2nd_field"),
            protobufSchema.messageTypes[0].fields.map { it.name },
        )
    }

    @Test
    fun fromConnectSchema_nestedStructsUnderADottedSchemaName_resolveTheirTypeReferences() {
        val schemaName = "my-server.inventory.customers.Value"
        val schemaFullName = "$AUTOGENERATED_PACKAGE.$schemaName"
        val address =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("$schemaFullName.Address")
                .field("street", Schema.STRING_SCHEMA)
                .build()
        val customer =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("$schemaFullName.Value.Customer")
                .field("name", Schema.STRING_SCHEMA)
                .build()
        val connectSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name(schemaName)
                .field("address", address)
                .field("customer", customer)
                .build()

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)

        val value = protobufSchema.findMessageTypeByName("Value")
        assertEquals(listOf("Customer"), value.nestedTypes.map { it.name })
        assertEquals(protobufSchema.findMessageTypeByName("Address"), value.findFieldByName("address").messageType)
        assertEquals(value.nestedTypes[0], value.findFieldByName("customer").messageType)
    }

    @Test
    fun fromConnectSchema_dottedSchemaName_roundTripsThroughProtobufAndBack() {
        val connectSchema =
            SchemaBuilder(Schema.Type.STRUCT)
                .name("my-server.inventory.customers.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("ts.ms", Schema.INT64_SCHEMA)
                .build()
        val connectData =
            Struct(connectSchema)
                .put("id", 42)
                .put("ts.ms", 1662000000000L)

        val protobufSchema = CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema)
        val protobufData = ConnectDataToProtobufDataConverter().convert(protobufSchema, connectSchema, connectData)

        val roundTrippedSchema = ProtobufSchemaToConnectSchemaConverter().toConnectSchema(protobufData)
        val roundTrippedData = ProtobufDataToConnectDataConverter().toConnectData(protobufData, roundTrippedSchema)

        assertEquals("Value", roundTrippedSchema.name())
        assertEquals(listOf("id", "ts_ms"), roundTrippedSchema.fields().map { it.name() })
        assertEquals(42, (roundTrippedData as Struct).get("id"))
        assertEquals(1662000000000L, roundTrippedData.get("ts_ms"))
    }

    @ParameterizedTest
    @ValueSource(strings = ["Value", "_Value", "Value2", "inventory.customers.Value", "com.example._1st"])
    fun toValidFullName_onNamesThatAreAlreadyValid_returnsThemUnchanged(name: String) {
        assertEquals(name, ProtobufSchemaConverterUtils.toValidFullName(name))
    }

    companion object {
        private const val AUTOGENERATED_PACKAGE =
            "com.amazonaws.services.schemaregistry.kafkaconnect.autogenerated"
        private val CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER = ConnectSchemaToProtobufSchemaConverter()

        private fun getPrimitiveTypesForExceptions(): Map<String, Schema> = linkedMapOf(
            "nonNumberTag" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "jsf").build(),
            "nullNumberTag" to SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, nullOf()).build(),
            "invalidInt32Metadata" to
                SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "int64").build(),
            "invalidInt64Metadata" to
                SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "string").build(),
        )

        @JvmStatic
        fun getConnectSchemaTestCases(): Stream<Arguments> = Stream.of(
            Arguments.of(
                "PrimitiveTypes",
                getPrimitiveSchema("PrimitiveTypes"),
                getProtobufSchema("PrimitiveProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "EnumType",
                getEnumSchema("EnumType"),
                getProtobufSchema("EnumProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "ArrayType",
                getArraySchema("ArrayType"),
                getProtobufSchema("ArrayProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "MapType",
                getMapSchema("MapType"),
                getProtobufSchema("MapProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "TimeType",
                getTimeSchema("TimeType"),
                getProtobufSchema("TimeProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "DecimalType",
                getDecimalSchema("DecimalType"),
                getProtobufSchema("DecimalProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "NestedType",
                getStructSchema("NestedType"),
                getProtobufSchema("NestedProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "OneofType",
                getOneofSchema("OneofType"),
                getProtobufSchema("OneofProtobufSchema.filedescproto"),
            ),
            Arguments.of(
                "AllTypes",
                getAllTypesSchema("AllTypes"),
                getProtobufSchema("AllTypesProtobufSchema.filedescproto"),
            ),
        )

        @JvmStatic
        fun getConnectSchemaExceptionTestCases(): Stream<Arguments> = getPrimitiveTypesForExceptions()
            .entries
            .stream()
            .map { entry -> Arguments.of(entry.key, mapOf(entry.key to entry.value)) }
    }
}
