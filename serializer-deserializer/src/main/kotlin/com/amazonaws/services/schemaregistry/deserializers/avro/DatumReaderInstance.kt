package com.amazonaws.services.schemaregistry.deserializers.avro

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AVROUtils
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

object DatumReaderInstance {
    private val log = LoggerFactory.getLogger(DatumReaderInstance::class.java)
    private val AVRO_UTILS: AVROUtils = AVROUtils.getInstance()

    /**
     * Creates the Avro datum reader used for deserialization. GenericDatumReader by default;
     * SpecificDatumReader only when the user asks for it, which requires the code-generated
     * schema class to be present locally.
     */
    @JvmStatic
    @Throws(InstantiationException::class, IllegalAccessException::class)
    fun from(
        writerSchemaDefinition: String,
        avroRecordType: AvroRecordType,
    ): DatumReader<Any> {
        val writerSchema = AVRO_UTILS.parseSchema(writerSchemaDefinition)

        return when (avroRecordType) {
            AvroRecordType.SPECIFIC_RECORD -> {
                val resolvedClass =
                    SpecificData.get().getClass(writerSchema)
                        ?: throw AWSSchemaRegistryException(
                            "Avro schema \"${writerSchema.fullName}\" has no generated class on the classpath. " +
                                "Deserializing as SPECIFIC_RECORD requires the class generated from that schema to be " +
                                "on the classpath; add it, or set ${AWSSchemaRegistryConstants.AVRO_RECORD_TYPE} to " +
                                "${AvroRecordType.GENERIC_RECORD.getName()}.",
                        )

                @Suppress("UNCHECKED_CAST")
                val readerClass = resolvedClass as Class<SpecificRecord>
                val readerSchema = readerClass.newInstance().schema
                log.debug("Using SpecificDatumReader for de-serializing Avro message, schema: {})", readerSchema.toString())
                SpecificDatumReader(writerSchema, readerSchema)
            }

            AvroRecordType.GENERIC_RECORD -> {
                log.debug("Using GenericDatumReader for de-serializing Avro message, schema: {})", writerSchema.toString())
                GenericDatumReader(writerSchema)
            }

            else -> throw UnsupportedOperationException("Unsupported AvroRecordType: ${avroRecordType.getName()}")
        }
    }
}
