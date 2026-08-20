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

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

object CommonTestHelper {
    @JvmStatic
    fun createConnectSchema(
        name: String,
        fieldSchemaMap: Map<String, Schema>,
        parameters: Map<String, String>,
    ): Schema {
        val parentSchemaBuilder = SchemaBuilder(Schema.Type.STRUCT)
        parentSchemaBuilder.name(name)
        parentSchemaBuilder.parameters(parameters)
        parentSchemaBuilder.version(1)

        fieldSchemaMap.forEach { (fieldName, schema) -> parentSchemaBuilder.field(fieldName, schema) }

        return parentSchemaBuilder.build()
    }
}
