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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import software.amazon.awssdk.services.glue.model.Compatibility
import java.util.stream.Stream

class JsonSchemaCompatibilityCheckerTest {
    private val checker = JsonSchemaCompatibilityChecker()

    @ParameterizedTest
    @EnumSource(
        value = Compatibility::class,
        names = ["NONE", "DISABLED", "UNKNOWN_TO_SDK_VERSION"],
    )
    fun testCheckCompatibility_reportsNothingWhenEnforcementIsOff(compatibility: Compatibility) {
        val errors = checker.checkCompatibility(REQUIRES_NAME_AND_AGE, REQUIRES_NAME, compatibility)

        assertEquals(emptyList<String>(), errors)
    }

    @Test
    fun testCheckCompatibility_reportsNothingWhenTheModeIsNull() {
        assertEquals(emptyList<String>(), checker.checkCompatibility(REQUIRES_NAME_AND_AGE, REQUIRES_NAME, null))
    }

    @ParameterizedTest
    @EnumSource(value = Compatibility::class, names = ["BACKWARD", "BACKWARD_ALL"])
    fun testBackward_rejectsAFieldThatBecomesRequired(compatibility: Compatibility) {
        val errors = checker.checkCompatibility(REQUIRES_NAME_AND_AGE, REQUIRES_NAME, compatibility)

        assertEquals(1, errors.size)
        assertTrue(errors[0].startsWith("BACKWARD incompatible"))
        assertTrue(errors[0].contains("'age'"))
    }

    @ParameterizedTest
    @EnumSource(value = Compatibility::class, names = ["BACKWARD", "BACKWARD_ALL"])
    fun testBackward_acceptsARequiredFieldThatBecomesOptional(compatibility: Compatibility) {
        val errors = checker.checkCompatibility(REQUIRES_NAME, REQUIRES_NAME_AND_AGE, compatibility)

        assertEquals(emptyList<String>(), errors)
    }

    @ParameterizedTest
    @EnumSource(value = Compatibility::class, names = ["FORWARD", "FORWARD_ALL"])
    fun testForward_rejectsARequiredFieldThatBecomesOptional(compatibility: Compatibility) {
        val errors = checker.checkCompatibility(REQUIRES_NAME, REQUIRES_NAME_AND_AGE, compatibility)

        assertEquals(1, errors.size)
        assertTrue(errors[0].startsWith("FORWARD incompatible"))
        assertTrue(errors[0].contains("'age'"))
    }

    @ParameterizedTest
    @EnumSource(value = Compatibility::class, names = ["FORWARD", "FORWARD_ALL"])
    fun testForward_acceptsAFieldThatBecomesRequired(compatibility: Compatibility) {
        val errors = checker.checkCompatibility(REQUIRES_NAME_AND_AGE, REQUIRES_NAME, compatibility)

        assertEquals(emptyList<String>(), errors)
    }

    @ParameterizedTest
    @EnumSource(value = Compatibility::class, names = ["FULL", "FULL_ALL"])
    fun testFull_rejectsAChangeInEitherDirection(compatibility: Compatibility) {
        assertEquals(1, checker.checkCompatibility(REQUIRES_NAME_AND_AGE, REQUIRES_NAME, compatibility).size)
        assertEquals(1, checker.checkCompatibility(REQUIRES_NAME, REQUIRES_NAME_AND_AGE, compatibility).size)
    }

    @Test
    fun testCheckCompatibility_acceptsAnIdenticalSchema() {
        assertEquals(
            emptyList<String>(),
            checker.checkCompatibility(REQUIRES_NAME, REQUIRES_NAME, Compatibility.FULL),
        )
    }

    @Test
    fun testCheckCompatibility_acceptsANewOptionalField() {
        val withOptionalAge =
            """{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},""" +
                """"required":["name"]}"""

        assertEquals(
            emptyList<String>(),
            checker.checkCompatibility(withOptionalAge, REQUIRES_NAME, Compatibility.FULL),
        )
    }

    @ParameterizedTest
    @MethodSource("definitionKeywords")
    fun testCheckCompatibility_looksInsideNamedDefinitions(keyword: String) {
        val previous = definitionSchema(keyword, """"required":["name"]""")
        val current = definitionSchema(keyword, """"required":["name","age"]""")

        val errors = checker.checkCompatibility(current, previous, Compatibility.BACKWARD)

        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("'$keyword.Person.age'"), errors[0])
    }

    @Test
    fun testCheckCompatibility_namesTheWholePathOfANestedDefinition() {
        val previous = nestedDefinitionSchema(""""required":["name"]""")
        val current = nestedDefinitionSchema(""""required":["name","age"]""")

        val errors = checker.checkCompatibility(current, previous, Compatibility.BACKWARD)

        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("'definitions.Outer.\u0024defs.Inner.age'"), errors[0])
    }

    @Test
    fun testCheckCompatibility_readsASchemaWrappedInItsName() {
        val previous = """{"Person":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}"""
        val current =
            """{"Person":{"type":"object","properties":{"name":{"type":"string"},""" +
                """"age":{"type":"integer"}},"required":["name","age"]}}"""

        val errors = checker.checkCompatibility(current, previous, Compatibility.BACKWARD)

        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("'age'"))
    }

    @Test
    fun testCheckCompatibility_treatsAnAbsentRequiredArrayAsNoRequiredField() {
        val noRequired = """{"type":"object","properties":{"name":{"type":"string"}}}"""

        assertEquals(emptyList<String>(), checker.checkCompatibility(noRequired, noRequired, Compatibility.FULL))
        assertEquals(1, checker.checkCompatibility(REQUIRES_NAME, noRequired, Compatibility.BACKWARD).size)
    }

    @Test
    fun testCheckCompatibility_reportsUnparseableInputRatherThanRaising() {
        val errors = checker.checkCompatibility("not json", REQUIRES_NAME, Compatibility.BACKWARD)

        assertEquals(1, errors.size)
        assertTrue(errors[0].startsWith("Failed to parse schema"))
    }

    private fun nestedDefinitionSchema(required: String): String = """{"definitions":{"Outer":{"type":"object",""" +
        """"${'$'}defs":{"Inner":{"type":"object",""" +
        """"properties":{"name":{"type":"string"},"age":{"type":"integer"}},$required}}}}}"""

    private fun definitionSchema(
        keyword: String,
        required: String,
    ): String = """{"$keyword":{"Person":{"type":"object",""" +
        """"properties":{"name":{"type":"string"},"age":{"type":"integer"}},$required}}}"""

    companion object {
        private const val REQUIRES_NAME =
            """{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}"""

        private const val REQUIRES_NAME_AND_AGE =
            """{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},""" +
                """"required":["name","age"]}"""

        @JvmStatic
        fun definitionKeywords(): Stream<String> = Stream.of("definitions", "\$defs")
    }
}
