package com.amazonaws.services.schemaregistry.serializers.protobuf

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException
import java.nio.file.Paths

object ProtobufTestCaseReader {
    const val TEST_METADATA_PATH = "src/test/resources/protobuf/"
    const val TEST_PROTO_PATH = "src/test/proto/"

    @JvmStatic
    fun getTestCases(): List<ProtobufTestCase> {
        val objectMapper = ObjectMapper()
        try {
            return objectMapper.readValue(
                Paths.get(TEST_METADATA_PATH + "TestMetadata.json").toFile(),
                object : TypeReference<List<ProtobufTestCase>>() {},
            )
        } catch (e: IOException) {
            throw RuntimeException("Error parsing test metadata JSON file", e)
        }
    }

    @JvmStatic
    fun getTestCasesByNames(vararg names: String): List<ProtobufTestCase> = names.map { getTestCaseByName(it) }

    @JvmStatic
    fun getTestCaseByName(name: String): ProtobufTestCase {
        val testCases = getTestCases()
        return testCases
            .firstOrNull { testCase -> testCase.fileName == name }
            ?: throw RuntimeException("Test case file not found: $name")
    }
}
