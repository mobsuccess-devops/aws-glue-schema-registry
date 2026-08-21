/*
 * Copyright 2026 Mobsuccess.
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

package com.amazonaws.services.schemaregistry.utils.apicurio

import com.squareup.wire.schema.internal.parser.ProtoFileElement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import java.util.Optional

class ProtobufSchemaTest {
    @Test
    fun testProtobufFile_isBuiltOnceAndReused() {
        val schema = protobufSchema()

        assertSame(schema.protobufFile, schema.protobufFile)
    }

    @Test
    fun testProtobufFile_describesTheSchemaItWasBuiltFrom() {
        val schema = protobufSchema()

        assertEquals("com.example", schema.protobufFile.getPackageName())
        assertEquals(setOf("id", "name"), schema.protobufFile.getFieldMap()["User"]?.keys)
    }

    @Test
    fun testProperties_returnWhatTheSchemaWasBuiltFrom() {
        val element = protoFileElement()
        val descriptor = FileDescriptorUtils.protoFileToFileDescriptor(element)

        val schema = ProtobufSchema(descriptor, element)

        assertSame(descriptor, schema.fileDescriptor)
        assertSame(element, schema.protoFileElement)
    }

    private fun protobufSchema(): ProtobufSchema {
        val element = protoFileElement()
        return ProtobufSchema(FileDescriptorUtils.protoFileToFileDescriptor(element), element)
    }

    private fun protoFileElement(): ProtoFileElement = ProtobufSchemaLoader
        .loadSchema(Optional.of("com.example"), FILE_NAME, SCHEMA)
        .getProtoFile()
        .toElement()

    private companion object {
        private const val FILE_NAME = "User.proto"
        private val SCHEMA =
            """
            syntax = "proto3";
            package com.example;

            message User {
                string id = 1;
                string name = 2;
            }
            """.trimIndent()
    }
}
