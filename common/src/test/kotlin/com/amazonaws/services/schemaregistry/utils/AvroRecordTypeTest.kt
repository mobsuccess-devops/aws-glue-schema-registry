package com.amazonaws.services.schemaregistry.utils

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AvroRecordTypeTest {
    @Test
    fun testForName_succeeds() {
        assertEquals(AvroRecordType.UNKNOWN, AvroRecordType.fromName("UNKNOWN"))
        assertEquals(AvroRecordType.SPECIFIC_RECORD, AvroRecordType.fromName("SPECIFIC_RECORD"))
        assertEquals(AvroRecordType.GENERIC_RECORD, AvroRecordType.fromName("GENERIC_RECORD"))
    }

    @Test
    fun testGetValue_returnsCorrectValue() {
        assertEquals(0, AvroRecordType.UNKNOWN.value)
        assertEquals(1, AvroRecordType.SPECIFIC_RECORD.value)
        assertEquals(2, AvroRecordType.GENERIC_RECORD.value)
    }
}
