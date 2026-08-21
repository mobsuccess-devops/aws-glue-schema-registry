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

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class SchemaValidatorBuilderTest {
    @Test
    fun testCanReadStrategy_asksWhetherTheCandidateCanReadTheExisting() {
        val candidate = FakeSchema("candidate", incompatibleWith = setOf("existing"))
        val existing = FakeSchema("existing", incompatibleWith = setOf("candidate"))

        val errors = SchemaValidatorBuilder().canReadStrategy().validateLatest().validate(candidate, listOf(existing))

        assertEquals(listOf("candidate cannot read existing"), errors)
    }

    @Test
    fun testCanBeReadStrategy_asksWhetherTheExistingCanReadTheCandidate() {
        val candidate = FakeSchema("candidate", incompatibleWith = setOf("existing"))
        val existing = FakeSchema("existing", incompatibleWith = setOf("candidate"))

        val errors = SchemaValidatorBuilder().canBeReadStrategy().validateLatest().validate(candidate, listOf(existing))

        assertEquals(listOf("existing cannot read candidate"), errors)
    }

    @Test
    fun testMutualReadStrategy_reportsBothDirectionsExistingFirst() {
        val candidate = FakeSchema("candidate", incompatibleWith = setOf("existing"))
        val existing = FakeSchema("existing", incompatibleWith = setOf("candidate"))

        val errors =
            SchemaValidatorBuilder().mutualReadStrategy().validateLatest().validate(candidate, listOf(existing))

        assertEquals(listOf("existing cannot read candidate", "candidate cannot read existing"), errors)
    }

    @Test
    fun testValidateLatest_looksAtTheFirstSchemaOnly() {
        val candidate = FakeSchema("candidate", incompatibleWith = setOf("first", "second"))
        val first = FakeSchema("first")
        val second = FakeSchema("second")

        val errors =
            SchemaValidatorBuilder().canReadStrategy().validateLatest().validate(candidate, listOf(first, second))

        assertEquals(listOf("candidate cannot read first"), errors)
        assertEquals(listOf<ParsedSchema>(first), candidate.comparedAgainst)
    }

    @Test
    fun testValidateLatest_acceptsAnEmptyHistory() {
        val candidate = FakeSchema("candidate")

        val errors = SchemaValidatorBuilder().canReadStrategy().validateLatest().validate(candidate, emptyList())

        assertEquals(emptyList<String>(), errors)
    }

    @Test
    fun testValidateAll_stopsAtTheFirstSchemaThatReportsSomething() {
        val candidate = FakeSchema("candidate", incompatibleWith = setOf("broken", "neverReached"))
        val clean = FakeSchema("clean")
        val broken = FakeSchema("broken")
        val neverReached = FakeSchema("neverReached")

        val errors =
            SchemaValidatorBuilder()
                .canReadStrategy()
                .validateAll()
                .validate(candidate, listOf(clean, broken, neverReached))

        assertEquals(listOf("candidate cannot read broken"), errors)
        assertEquals(listOf<ParsedSchema>(clean, broken), candidate.comparedAgainst)
    }

    @Test
    fun testValidateAll_reportsNothingWhenEverySchemaIsClean() {
        val candidate = FakeSchema("candidate")
        val schemas = listOf(FakeSchema("a"), FakeSchema("b"))

        val errors = SchemaValidatorBuilder().canReadStrategy().validateAll().validate(candidate, schemas)

        assertEquals(emptyList<String>(), errors)
        assertEquals(schemas, candidate.comparedAgainst)
    }

    @Test
    fun testValidator_refusesToBuildWithoutAStrategy() {
        assertThrows(RuntimeException::class.java) { SchemaValidatorBuilder().validateLatest() }
        assertThrows(RuntimeException::class.java) { SchemaValidatorBuilder().validateAll() }
    }

    private class FakeSchema(
        private val label: String,
        private val incompatibleWith: Set<String> = emptySet(),
    ) : ParsedSchema {
        val comparedAgainst: MutableList<ParsedSchema> = mutableListOf()

        override fun schemaType(): String = "FAKE"

        override fun name(): String = label

        override fun canonicalString(): String = label

        override fun isBackwardCompatible(previousSchema: ParsedSchema): List<String> {
            comparedAgainst.add(previousSchema)
            return if (previousSchema.name() in incompatibleWith) {
                listOf("$label cannot read ${previousSchema.name()}")
            } else {
                emptyList()
            }
        }

        override fun rawSchema(): Any = label

        override fun toString(): String = label
    }
}
