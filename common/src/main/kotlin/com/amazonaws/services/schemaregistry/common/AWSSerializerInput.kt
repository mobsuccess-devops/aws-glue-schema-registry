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

package com.amazonaws.services.schemaregistry.common

/**
 * Encapsulates general inputs for serializer
 */
class AWSSerializerInput(
    schemaDefinition: String?,
    schemaName: String?,
    dataFormat: String?,
    transportName: String?,
) {
    val schemaDefinition: String? = schemaDefinition
    val dataFormat: String? = dataFormat
    val transportName: String? = transportName ?: DEFAULT_TRANSPORT_NAME

    // Le nom de schéma par défaut dérive du transportName **reçu**, pas de sa valeur
    // repliée sur "default-stream" : passer un transportName nul donne donc un
    // schemaName nul. Comportement du code d'origine, couvert par un test.
    val schemaName: String? = schemaName ?: AWSSchemaNamingStrategyDefaultImpl().getSchemaName(transportName)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AWSSerializerInput) return false
        return schemaDefinition == other.schemaDefinition &&
            schemaName == other.schemaName &&
            dataFormat == other.dataFormat &&
            transportName == other.transportName
    }

    override fun hashCode(): Int {
        var result = schemaDefinition?.hashCode() ?: 0
        result = 31 * result + (schemaName?.hashCode() ?: 0)
        result = 31 * result + (dataFormat?.hashCode() ?: 0)
        result = 31 * result + (transportName?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String = "AWSSerializerInput(schemaDefinition=$schemaDefinition, schemaName=$schemaName, " +
        "dataFormat=$dataFormat, transportName=$transportName)"

    /** Reprend l'API fluide que générait Lombok : appelée depuis du code Java. */
    class Builder internal constructor() {
        private var schemaDefinition: String? = null
        private var schemaName: String? = null
        private var dataFormat: String? = null
        private var transportName: String? = null

        fun schemaDefinition(schemaDefinition: String?): Builder = apply { this.schemaDefinition = schemaDefinition }

        fun schemaName(schemaName: String?): Builder = apply { this.schemaName = schemaName }

        fun dataFormat(dataFormat: String?): Builder = apply { this.dataFormat = dataFormat }

        fun transportName(transportName: String?): Builder = apply { this.transportName = transportName }

        fun build(): AWSSerializerInput = AWSSerializerInput(schemaDefinition, schemaName, dataFormat, transportName)
    }

    companion object {
        private const val DEFAULT_TRANSPORT_NAME = "default-stream"

        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
