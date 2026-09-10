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

package com.amazonaws.services.schemaregistry.serializers.json

import com.kjetland.jackson.jsonSchema.JsonSchemaConfig

/**
 * Builds the [JsonSchemaConfig] the JSON serializer generates a POJO's schema with.
 *
 * An implementation is named by the `jsonSchemaConfig` configuration property, in place of a
 * [JsonSchemaConfig] instance where only a class name can be passed, and is instantiated by
 * reflection, so it needs a public no-argument constructor. The configuration it returns is
 * used as it is: the `jsonSchemaNullableEnabled` property is not applied on top of it.
 */
public fun interface JsonSchemaConfigFactory {
    /**
     * Returns the generator configuration.
     */
    public fun newJsonSchemaConfig(): JsonSchemaConfig
}
