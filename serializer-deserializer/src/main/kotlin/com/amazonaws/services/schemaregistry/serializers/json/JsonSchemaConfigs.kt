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

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig

internal object JsonSchemaConfigs {
    private const val KEY = AWSSchemaRegistryConstants.JSON_SCHEMA_CONFIG

    fun forConfiguration(configs: GlueSchemaRegistryConfiguration): JsonSchemaConfig? = when (val value = configs.jsonSchemaConfig) {
        null -> if (configs.isJsonSchemaNullableEnabled) JsonSchemaConfig.nullableJsonSchemaDraft4() else null
        is JsonSchemaConfig -> value
        is String -> factory(value).newJsonSchemaConfig()
        is Class<*> -> factory(value.name).newJsonSchemaConfig()
        else -> throw AWSSchemaRegistryException(
            "Configuration property $KEY must be a ${JsonSchemaConfig::class.java.name}, or the name of a " +
                "class implementing ${JsonSchemaConfigFactory::class.java.name}, not a ${value.javaClass.name}",
        )
    }

    private fun factory(className: String): JsonSchemaConfigFactory {
        val instance =
            try {
                loadClass(className).getDeclaredConstructor().newInstance()
            } catch (e: Exception) {
                throw notInstantiable(className, e)
            } catch (e: LinkageError) {
                throw notInstantiable(className, e)
            }
        return instance as? JsonSchemaConfigFactory
            ?: throw AWSSchemaRegistryException(
                "Configuration property $KEY has to name a class implementing " +
                    "${JsonSchemaConfigFactory::class.java.name}; $className does not.",
            )
    }

    private fun notInstantiable(
        className: String,
        cause: Throwable,
    ): AWSSchemaRegistryException = AWSSchemaRegistryException(
        "Configuration property $KEY names a class that could not be instantiated: $className. " +
            "It has to be a public class with a public no-argument constructor, implementing " +
            "${JsonSchemaConfigFactory::class.java.name}, and on the classpath.",
        cause,
    )

    private fun loadClass(className: String): Class<*> {
        val contextClassLoader = Thread.currentThread().contextClassLoader ?: return Class.forName(className)
        return try {
            Class.forName(className, true, contextClassLoader)
        } catch (_: ClassNotFoundException) {
            Class.forName(className)
        } catch (_: LinkageError) {
            Class.forName(className)
        }
    }
}
