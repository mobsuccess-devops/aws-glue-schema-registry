package com.amazonaws.services.schemaregistry.serializers.avro

import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class AvroSerializerTest {
    @Test
    fun serialize_WhenSerializeIsCalled_ReturnsCachedInstance() {
        val avroSerializer = AvroSerializer()

        val specificUserRecord = RecordGenerator.createSpecificAvroRecord()
        val genericUserRecord = RecordGenerator.createGenericUserMapAvroRecord()

        avroSerializer.serialize(specificUserRecord)
        avroSerializer.serialize(genericUserRecord)
        // Same schema won't be cached again.
        avroSerializer.serialize(genericUserRecord)

        assertEquals(2, avroSerializer.datumWriterCache.size())
    }
}
