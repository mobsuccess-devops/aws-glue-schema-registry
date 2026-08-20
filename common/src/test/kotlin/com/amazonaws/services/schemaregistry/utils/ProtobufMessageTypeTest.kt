package com.amazonaws.services.schemaregistry.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ProtobufMessageTypeTest {
    @Test
    fun test_Existing_Enum_Value() {
        assertEquals(ProtobufMessageType.POJO, ProtobufMessageType.fromName("POJO"))
        assertEquals(ProtobufMessageType.DYNAMIC_MESSAGE, ProtobufMessageType.fromName("DYNAMIC_MESSAGE"))
    }

    @Test
    fun test_Non_Existent_Enum_Value() {
        assertThrows(IllegalArgumentException::class.java) { ProtobufMessageType.fromName("Random") }
    }

    @Test
    fun test_Null_Enum_Value() {
        assertThrows(IllegalArgumentException::class.java) { ProtobufMessageType.fromName("") }
    }

    @Test
    fun test_GetName() {
        assertEquals("POJO", ProtobufMessageType.POJO.getName())
    }

    @Test
    fun test_GetValue() {
        assertEquals(1, ProtobufMessageType.POJO.value)
    }
}
