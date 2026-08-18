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

package com.amazonaws.services.schemaregistry.utils

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy
import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyDefaultImpl
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import org.apache.commons.lang3.Validate
import org.slf4j.LoggerFactory

class GlueSchemaRegistryUtils private constructor() {
    /**
     * Check value is present in the map or not.
     */
    fun checkIfPresentInMap(
        map: Map<String, *>,
        key: String,
    ): Boolean {
        Validate.notEmpty(map)
        return map.containsKey(key)
    }

    // Nullable comme en Java : initializeStrategy renvoie null quand la classe
    // chargée n'implémente pas l'interface.
    fun configureSchemaNamingStrategy(configs: Map<String, *>): AWSSchemaNamingStrategy? = if (isSchemaGenerationClassPresent(configs)) {
        useCustomerProvidedStrategy(
            configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS].toString(),
        )
    } else {
        useDefaultStrategy()
    }

    /**
     * Returns the schema Name.
     */
    fun getSchemaName(configs: Map<String, *>): String? = if (checkIfPresentInMap(configs, AWSSchemaRegistryConstants.SCHEMA_NAME)) {
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
    } else {
        null
    }

    /**
     * Returns the data format.
     */
    fun getDataFormat(configs: Map<String, *>): String {
        if (checkIfPresentInMap(configs, AWSSchemaRegistryConstants.DATA_FORMAT)) {
            return configs[AWSSchemaRegistryConstants.DATA_FORMAT].toString().uppercase()
        }
        throw AWSSchemaRegistryException("Unable to find configuration for dataFormat")
    }

    /**
     * Instantiates classes provided in the kafka properties.
     */
    private fun initializeStrategy(className: String): AWSSchemaNamingStrategy? {
        var schemaNameStrategy: AWSSchemaNamingStrategy? = null
        try {
            val newInstance = Class.forName(className).getDeclaredConstructor().newInstance()
            if (newInstance is AWSSchemaNamingStrategy) {
                schemaNameStrategy = newInstance
            }
        } catch (e: Exception) {
            val message =
                "Unable to locate the naming strategy class, check in the classpath for classname = $className"
            log.error(message, AWSSchemaRegistryConstants.DEFAULT_SCHEMA_STRATEGY)
            throw AWSSchemaRegistryException(message, e)
        }
        return schemaNameStrategy
    }

    private fun useDefaultStrategy(): AWSSchemaNamingStrategy = AWSSchemaNamingStrategyDefaultImpl()

    private fun useCustomerProvidedStrategy(className: String): AWSSchemaNamingStrategy? = initializeStrategy(className)

    private fun isSchemaGenerationClassPresent(configs: Map<String, *>): Boolean = checkIfPresentInMap(configs, AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS)

    companion object {
        private val log = LoggerFactory.getLogger(GlueSchemaRegistryUtils::class.java)
        private val INSTANCE = GlueSchemaRegistryUtils()

        /**
         * Thread safe singleton instance of the GlueSchemaRegistryUtils Class.
         */
        @JvmStatic
        fun getInstance(): GlueSchemaRegistryUtils = INSTANCE
    }
}
