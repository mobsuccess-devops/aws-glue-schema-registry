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

package com.amazonaws.services.schemaregistry.common.compatibility

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import software.amazon.awssdk.services.glue.model.Compatibility

/**
 * Compares two JSON schema definitions against a Glue compatibility mode, on the client side.
 *
 * Glue enforces the compatibility mode of a schema for Avro and Protobuf, but not for JSON: a
 * JSON schema version that breaks its declared mode is accepted by `RegisterSchemaVersion` and
 * the breakage surfaces in a consumer instead.
 *
 * What is compared is the `required` contract, at the top level and inside each named entry of
 * `definitions` or `$defs`: a field that becomes required breaks a reader of older data, and a
 * required field that stops being required breaks an older reader of new data. Everything else
 * a schema can say — types, formats, enumerations, `additionalProperties` — is **not** compared.
 * A report of no errors therefore means "no broken `required` contract", not "compatible".
 *
 * Nothing here is read from Glue. The mode to apply and the definition to compare against are
 * both supplied by the caller, and `AWSSchemaRegistryClient` supplies the `compatibility` of the
 * local configuration — which defaults to `BACKWARD` when the key is absent — together with the
 * latest version of the schema. A mode that does not match the one the schema carries in the
 * registry is therefore applied all the same, and a transitive mode compares the latest version
 * alone; `docs/configuration.md` states what that means for a caller.
 */
public class JsonSchemaCompatibilityChecker {
    /**
     * Returns one message per incompatibility found between [newSchemaDefinition] and
     * [previousSchemaDefinition] under [compatibility], or an empty list when none is found.
     *
     * The modes that disable enforcement — `NONE`, `DISABLED`, and a mode this SDK does not
     * know — return an empty list without reading either definition.
     *
     * The transitive modes `BACKWARD_ALL`, `FORWARD_ALL` and `FULL_ALL` are checked as
     * `BACKWARD`, `FORWARD` and `FULL`: [previousSchemaDefinition] is the only definition
     * compared against, so passing the latest version of a schema says nothing about the
     * versions registered before it.
     */
    public fun checkCompatibility(
        newSchemaDefinition: String,
        previousSchemaDefinition: String,
        compatibility: Compatibility?,
    ): List<String> {
        val directions = directionsOf(compatibility)
        if (directions.isEmpty()) {
            return emptyList()
        }

        val newSchema: JsonNode
        val previousSchema: JsonNode
        try {
            newSchema = MAPPER.readTree(newSchemaDefinition)
            previousSchema = MAPPER.readTree(previousSchemaDefinition)
        } catch (e: Exception) {
            return listOf("Failed to parse schema: ${e.message}")
        }

        return directions.flatMap { direction -> check(newSchema, previousSchema, direction, "") }
    }

    private fun check(
        newSchema: JsonNode,
        previousSchema: JsonNode,
        direction: Direction,
        path: String,
    ): List<String> {
        val newRequired = requiredFields(newSchema)
        val previousRequired = requiredFields(previousSchema)

        val errors =
            when (direction) {
                Direction.BACKWARD ->
                    (newRequired - previousRequired).map { field ->
                        "BACKWARD incompatible: field '${qualify(path, field)}' is now required but was not " +
                            "required in the previous schema. Data written against the previous schema may " +
                            "not carry it."
                    }

                Direction.FORWARD ->
                    (previousRequired - newRequired).map { field ->
                        "FORWARD incompatible: field '${qualify(path, field)}' was required and is now " +
                            "optional or absent. A reader of the previous schema expects it to be present."
                    }
            }

        return errors + checkDefinitions(newSchema, previousSchema, direction, path)
    }

    private fun checkDefinitions(
        newSchema: JsonNode,
        previousSchema: JsonNode,
        direction: Direction,
        path: String,
    ): List<String> {
        val newDefinitions = definitions(newSchema) ?: return emptyList()
        val previousDefinitions = definitions(previousSchema) ?: return emptyList()

        return previousDefinitions.node
            .fields()
            .asSequence()
            .mapNotNull { (name, previousDefinition) ->
                newDefinitions.node.get(name)?.let { newDefinition ->
                    check(
                        newDefinition,
                        previousDefinition,
                        direction,
                        qualify(qualify(path, newDefinitions.keyword), name),
                    )
                }
            }.flatten()
            .toList()
    }

    private fun requiredFields(schema: JsonNode): Set<String> {
        val required = unwrap(schema).get(REQUIRED) ?: return emptySet()
        if (!required.isArray) {
            return emptySet()
        }
        return required.map { it.asText() }.toSet()
    }

    private fun definitions(schema: JsonNode): Definitions? {
        val schemaNode = unwrap(schema)
        schemaNode.get(DEFINITIONS)?.let { return Definitions(DEFINITIONS, it) }
        return schemaNode.get(DEFS)?.let { Definitions(DEFS, it) }
    }

    private class Definitions(
        val keyword: String,
        val node: JsonNode,
    )

    private fun unwrap(schema: JsonNode): JsonNode {
        if (schema.has(TYPE) || schema.has(SCHEMA_KEYWORD) || schema.has(PROPERTIES)) {
            return schema
        }

        val candidates =
            schema
                .fields()
                .asSequence()
                .filter { (name, _) -> name != DEFINITIONS && name != DEFS }
                .map { (_, value) -> value }
                .toList()

        val onlyCandidate = candidates.singleOrNull() ?: return schema
        return if (onlyCandidate.has(TYPE) || onlyCandidate.has(PROPERTIES)) onlyCandidate else schema
    }

    private fun qualify(
        path: String,
        field: String,
    ): String = if (path.isEmpty()) field else "$path.$field"

    private enum class Direction {
        BACKWARD,
        FORWARD,
    }

    private companion object {
        private val MAPPER = ObjectMapper()

        private const val REQUIRED = "required"
        private const val DEFINITIONS = "definitions"
        private const val DEFS = "\$defs"
        private const val TYPE = "type"
        private const val PROPERTIES = "properties"
        private const val SCHEMA_KEYWORD = "\$schema"

        private fun directionsOf(compatibility: Compatibility?): List<Direction> = when (compatibility) {
            Compatibility.BACKWARD, Compatibility.BACKWARD_ALL -> listOf(Direction.BACKWARD)
            Compatibility.FORWARD, Compatibility.FORWARD_ALL -> listOf(Direction.FORWARD)
            Compatibility.FULL, Compatibility.FULL_ALL -> listOf(Direction.BACKWARD, Direction.FORWARD)
            else -> emptyList()
        }
    }
}
