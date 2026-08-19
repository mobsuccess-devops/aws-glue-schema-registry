package com.amazonaws.services.schemaregistry.serializers.avro

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.DatumWriter
import org.apache.avro.specific.SpecificDatumWriter

object DatumWriterInstance {
    @JvmStatic
    fun get(
        schema: Schema,
        avroRecordType: AvroRecordType,
    ): DatumWriter<Any> = when (avroRecordType) {
        AvroRecordType.SPECIFIC_RECORD -> SpecificDatumWriter(schema)
        AvroRecordType.GENERIC_RECORD -> GenericDatumWriter(schema)
        else -> throw AWSSchemaRegistryException("Unsupported type passed for serialization: $avroRecordType")
    }
}
