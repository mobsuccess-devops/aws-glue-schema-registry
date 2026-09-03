package com.amazonaws.services.schemaregistry.serializers.json

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.MissingNode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.atLeastOnce
import org.mockito.kotlin.spy
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
    }
}
