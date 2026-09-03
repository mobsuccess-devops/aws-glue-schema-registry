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

package com.amazonaws.services.schemaregistry.common.configs

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Builds the [ObjectMapper] the JSON serializer and deserializer read and write with.
 *
 * An implementation is named by the `objectMapperFactory` configuration property and is
 * instantiated by reflection, so it needs a public no-argument constructor. It is the hook for
 * everything the `jacksonSerializationFeatures` and `jacksonDeserializationFeatures` properties
 * cannot express: registering a module, setting a `MapperFeature`, installing a custom
 * `SerializerProvider`, and so on.
 *
 * [newObjectMapper] is called once per serializer and once per deserializer, and must return a
 * fresh instance each time: the caller goes on to configure the mapper it is given, and two
 * serdes sharing one mapper would then configure each other's.
 *
 * The mapper returned is configured further by the library, in this order: the module named by
 * `registerJavaTimeModule`, then the Jackson feature properties. A feature this factory sets is
 * therefore overridden by the same feature named in those properties.
 */
public interface ObjectMapperFactory {
    /**
     * Returns a new [ObjectMapper], never one already handed out.
     */
    public fun newObjectMapper(): ObjectMapper
}
