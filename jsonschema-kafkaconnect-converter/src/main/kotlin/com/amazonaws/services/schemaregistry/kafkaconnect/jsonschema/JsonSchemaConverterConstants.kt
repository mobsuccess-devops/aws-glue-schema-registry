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

package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema

/**
 * Glue Schema Registry JSON Schema converter constants.
 */
object JsonSchemaConverterConstants {
    const val NAMESPACE = "com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema"
    const val JSON_SCHEMA_TYPE_ENUM = "$NAMESPACE.Enum"
    const val KEY_FIELD = "key"
    const val VALUE_FIELD = "value"
    const val JSON_FIELD_DEFAULT_FLAG_PROP = "$NAMESPACE.field.default"
    const val CONNECT_NAME_PROP = "connect.name"
    const val CONNECT_DOC_PROP = "connect.doc"
    const val CONNECT_VERSION_PROP = "connect.version"
    const val CONNECT_PARAMETERS_PROP = "connect.parameters"
    const val CONNECT_TYPE_PROP = "connect.type"
    const val CONNECT_INDEX_PROP = "connect.index"
    const val JSON_SCHEMA_TYPE_ONEOF = "oneOf"
}
