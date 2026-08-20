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
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.nullOf
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.errors.DataException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
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

    companion object {
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
