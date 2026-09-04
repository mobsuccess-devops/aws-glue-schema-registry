package com.amazonaws.services.schemaregistry.serializers.json

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.MissingNode
import org.everit.json.schema.BooleanSchema
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify

class JsonValidatorTest {
    private val validator = JsonValidator()
    private val mapper = ObjectMapper()
    private val stringSchema =
        """
        {
          "${'$'}schema": "http://json-schema.org/draft-07/schema#",
          "description": "String schema",
          "type": "string"
        }
        """.trimIndent()

    @Test
    fun testBinaryNode() {
        val bytes = "Test String".toByteArray()
        val dataNode: JsonNode = BinaryNode(bytes)
        assertEquals(dataNode.nodeType, JsonNodeType.BINARY)

        val schemaNode = mapper.readTree(stringSchema)
        validator.validateDataWithSchema(schemaNode, dataNode)
    }

    /**
     * The serializer hands the validator the mapper it was configured with, so that the text the
     * schema and the data are validated as is written the way the rest of the serializer writes.
     */
    @Test
    fun testValidateDataWithSchema_writesThroughTheMapperItWasGiven() {
        val configuredMapper = spy(ObjectMapper())
        val schemaNode = mapper.readTree(stringSchema)

        JsonValidator(configuredMapper).validateDataWithSchema(schemaNode, mapper.readTree(A_STRING_JSON))

        verify(configuredMapper, atLeastOnce()).writeValueAsString(schemaNode)
    }

    /**
     * The schema is parsed once per definition and reused afterwards, while the schema node is
     * still written out on every call, so a caller changing the node is still followed.
     */
    @Test
    fun testValidateDataWithSchema_reusesTheParsedSchema() {
        val configuredMapper = spy(ObjectMapper())
        val validator = JsonValidator(configuredMapper)
        val schemaNode = mapper.readTree(stringSchema)

        validator.validateDataWithSchema(schemaNode, mapper.readTree(A_STRING_JSON))
        val parsed = validator.parsedSchemaCache.getIfPresent(mapper.writeValueAsString(schemaNode))
        validator.validateDataWithSchema(schemaNode, mapper.readTree(ANOTHER_STRING_JSON))

        assertNotNull(parsed)
        assertSame(parsed, validator.parsedSchemaCache.getIfPresent(mapper.writeValueAsString(schemaNode)))
        verify(configuredMapper, times(2)).writeValueAsString(schemaNode)
    }

    /**
     * A cached entry, rather than the schema node, is what the data is validated against: a boolean
     * schema parked under the key of a string definition rejects a string.
     */
    @Test
    fun testValidateDataWithSchema_validatesAgainstTheCachedSchema() {
        val schemaNode = mapper.readTree(stringSchema)
        validator.parsedSchemaCache.put(
            mapper.writeValueAsString(schemaNode),
            BooleanSchema.builder().build(),
        )

        assertThrows(AWSSchemaRegistryException::class.java) {
            validator.validateDataWithSchema(schemaNode, mapper.readTree(A_STRING_JSON))
        }
    }

    /**
     * A malformed schema raises the same exception it always did, and the failure is not remembered.
     */
    @Test
    fun testValidateDataWithSchema_malformedSchemaThrowsAndIsNotCached() {
        val schemaNode = mapper.readTree(MALFORMED_SCHEMA)

        assertThrows(AWSSchemaRegistryException::class.java) {
            validator.validateDataWithSchema(schemaNode, mapper.readTree(A_STRING_JSON))
        }

        assertEquals(0L, validator.parsedSchemaCache.size())

        assertThrows(AWSSchemaRegistryException::class.java) {
            validator.validateDataWithSchema(schemaNode, mapper.readTree(A_STRING_JSON))
        }
    }

    @Test
    fun testMissingNode() {
        val dataNode: JsonNode = MissingNode.getInstance()
        assertEquals(dataNode.nodeType, JsonNodeType.MISSING)

        val schemaNode = mapper.readTree(stringSchema)
        assertThrows(AWSSchemaRegistryException::class.java) {
            validator.validateDataWithSchema(schemaNode, dataNode)
        }
    }

    private companion object {
        const val A_STRING_JSON = "\"a string\""
        const val ANOTHER_STRING_JSON = "\"another string\""
        const val MALFORMED_SCHEMA = """{"type": "nonsense"}"""
    }
}
