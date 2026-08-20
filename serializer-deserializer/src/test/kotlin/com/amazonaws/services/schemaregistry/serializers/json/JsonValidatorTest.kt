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

    @Test
    fun testMissingNode() {
        val dataNode: JsonNode = MissingNode.getInstance()
        assertEquals(dataNode.nodeType, JsonNodeType.MISSING)

        val schemaNode = mapper.readTree(stringSchema)
        assertThrows(AWSSchemaRegistryException::class.java) {
            validator.validateDataWithSchema(schemaNode, dataNode)
        }
    }
}
