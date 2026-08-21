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

import com.amazonaws.services.schemaregistry.kafkaconnect.config.GlueSchemaRegistryConfigDef
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import software.amazon.awssdk.services.glue.model.DataFormat

/**
 * Glue Schema Registry JSON Schema converter config.
 *
 * @param props property elements for the converter config
 */
class JsonSchemaConverterConfig(
    props: Map<String, *>,
) : AbstractConfig(CONFIG_DEF, GlueSchemaRegistryConfigDef.coerce(CONFIG_DEF, props)) {
    companion object {
        private val CONFIG_DEF: ConfigDef =
            GlueSchemaRegistryConfigDef
                .defineJson(
                    GlueSchemaRegistryConfigDef.defineDataFormat(
                        GlueSchemaRegistryConfigDef.baseConfigDef(),
                        DataFormat.JSON,
                    ),
                ).also { configDef ->
                    JsonSchemaDataConfig.baseConfigDef().configKeys().values.forEach { configDef.define(it) }
                }

        @JvmStatic
        fun configDef(): ConfigDef = ConfigDef(CONFIG_DEF)

        internal fun coerce(props: Map<String, *>): Map<String, *> = GlueSchemaRegistryConfigDef.coerce(CONFIG_DEF, props)
    }
}
