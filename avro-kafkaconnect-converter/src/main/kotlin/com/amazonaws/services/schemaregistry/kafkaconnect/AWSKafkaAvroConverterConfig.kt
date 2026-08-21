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

package com.amazonaws.services.schemaregistry.kafkaconnect

import com.amazonaws.services.schemaregistry.kafkaconnect.avrodata.AvroDataConfig
import com.amazonaws.services.schemaregistry.kafkaconnect.config.GlueSchemaRegistryConfigDef
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

/**
 * Amazon Schema Registry Avro converter config.
 *
 * @param props property elements for the converter config
 */
class AWSKafkaAvroConverterConfig(
    props: Map<String, *>,
) : AbstractConfig(CONFIG_DEF, GlueSchemaRegistryConfigDef.coerce(CONFIG_DEF, props)) {
    companion object {
        const val ASSUME_ROLE_SESSION_NAME_DEFAULT = "kafka-connect-session"

        private const val GROUP_ASSUME_ROLE = "Assume role"

        private val CONFIG_DEF: ConfigDef =
            GlueSchemaRegistryConfigDef
                .defineAvro(GlueSchemaRegistryConfigDef.baseConfigDef())
                .define(
                    AWSSchemaRegistryConstants.ASSUME_ROLE_ARN,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    "ARN of an IAM role the converter assumes before calling Glue. When unset, the " +
                        "credentials of the Connect worker are used directly.",
                    GROUP_ASSUME_ROLE,
                    1,
                    ConfigDef.Width.LONG,
                    "Assume role ARN",
                ).define(
                    AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION_NAME,
                    ConfigDef.Type.STRING,
                    ASSUME_ROLE_SESSION_NAME_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "Session name reported to STS when " + AWSSchemaRegistryConstants.ASSUME_ROLE_ARN +
                        " is set.",
                    GROUP_ASSUME_ROLE,
                    2,
                    ConfigDef.Width.MEDIUM,
                    "Assume role session name",
                ).also { configDef ->
                    AvroDataConfig.baseConfigDef().configKeys().values.forEach { configDef.define(it) }
                }

        @JvmStatic
        fun configDef(): ConfigDef = ConfigDef(CONFIG_DEF)

        internal fun coerce(props: Map<String, *>): Map<String, *> = GlueSchemaRegistryConfigDef.coerce(CONFIG_DEF, props)
    }
}
