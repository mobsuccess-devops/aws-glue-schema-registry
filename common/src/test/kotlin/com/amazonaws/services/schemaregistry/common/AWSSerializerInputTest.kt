package com.amazonaws.services.schemaregistry.common

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class AWSSerializerInputTest {
    @Test
    fun testBuilder_withSchemaNameAndTransportName_objectBuildSuccessfully() {
        val awsSerializerInput =
            AWSSerializerInput
                .builder()
                .transportName(TRANSPORT_NAME)
                .schemaName(SCHEMA_NAME)
                .build()

        assertEquals(SCHEMA_NAME, awsSerializerInput.schemaName)
        assertEquals(TRANSPORT_NAME, awsSerializerInput.transportName)
    }

    @Test
    fun testBuilder_withNullSchemaNameAndNullTransportName_objectBuildSuccessfully() {
        val awsSerializerInput =
            AWSSerializerInput
                .builder()
                .transportName(null)
                .schemaName(null)
                .build()

        assertNull(awsSerializerInput.schemaName)
        assertEquals(DEFAULT_TRANSPORT_NAME, awsSerializerInput.transportName)
    }

    companion object {
        private const val SCHEMA_NAME = "test-schema-name"
        private const val TRANSPORT_NAME = "test-transport-name"
        private const val DEFAULT_TRANSPORT_NAME = "default-stream"
    }
}
