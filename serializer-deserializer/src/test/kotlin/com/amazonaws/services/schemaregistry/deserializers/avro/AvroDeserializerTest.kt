/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.deserializers.avro

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.SchemaByDefinitionFetcher
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.exception.GlueSchemaRegistryIncompatibleDataException
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade
import com.amazonaws.services.schemaregistry.serializers.avro.User
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.AvroRecordType
import com.amazonaws.services.schemaregistry.utils.RecordGenerator
import com.amazonaws.services.schemaregistry.utils.SchemaLoader
import com.amazonaws.services.schemaregistry.utils.SerializedByteArrayGenerator
import com.amazonaws.services.schemaregistry.utils.nullOf
import org.apache.avro.AvroRuntimeException
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.junit.jupiter.MockitoSettings
import org.mockito.quality.Strictness
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.glue.model.DataFormat
import java.nio.ByteBuffer
import java.util.UUID

/**
 * Unit tests for testing Avro related serialization and de-serialization.
 */
@ExtendWith(MockitoExtension::class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AvroDeserializerTest {
    private val configs: MutableMap<String, Any?> = HashMap()

    @Mock
    lateinit var mockDefaultCredProvider: AwsCredentialsProvider

    @Mock
    private lateinit var mockSchemaByDefinitionFetcher: SchemaByDefinitionFetcher

    private lateinit var schemaRegistrySerDeConfigs: GlueSchemaRegistryConfiguration

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    fun setup() {
        this.configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        this.configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        this.schemaRegistrySerDeConfigs = GlueSchemaRegistryConfiguration(this.configs)

        MockitoAnnotations.openMocks(this)
    }

    /**
     * Helper method to serialize data for testing de-serialization.
     *
     * @param objectToSerialize object for serialization
     * @return serialized ByteBuffer of the object
     */
    fun createBasicSerializedData(
        objectToSerialize: Any,
        compressionType: String,
        dataFormat: DataFormat,
    ): ByteBuffer {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType
        val glueSchemaRegistrySerializationFacade =
            GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(this.mockDefaultCredProvider)
                .configs(configs)
                .schemaByDefinitionFetcher(mockSchemaByDefinitionFetcher)
                .build()
        return getByteBuffer(objectToSerialize, glueSchemaRegistrySerializationFacade, dataFormat)
    }

    private fun getByteBuffer(
        objectToSerialize: Any,
        glueSchemaRegistrySerializationFacade: GlueSchemaRegistrySerializationFacade,
        dataFormat: DataFormat,
    ): ByteBuffer {
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(
                dataFormat,
                objectToSerialize,
                TEST_GENERIC_SCHEMA_VERSION_ID,
            )
        return ByteBuffer.wrap(serializedData)
    }

    /**
     * Creates a AvroSerializer object for testing.
     *
     * @return AvroSerializer object instance
     */
    private fun createGlueSchemaRegistryFacade(compressionType: String): GlueSchemaRegistrySerializationFacade {
        configs[AWSSchemaRegistryConstants.COMPRESSION_TYPE] = compressionType
        return GlueSchemaRegistrySerializationFacade
            .builder()
            .credentialProvider(this.mockDefaultCredProvider)
            .configs(configs)
            .schemaByDefinitionFetcher(this.mockSchemaByDefinitionFetcher)
            .build()
    }

    /**
     * Creates and returns a de-serialized object.
     *
     * @param schema         schema for de-serialization
     * @param serializedData serialized data
     * @return de-serialized object
     */
    private fun createDeserializedObjectForGenericRecord(
        schema: Schema,
        serializedData: ByteArray,
    ): Any? {
        val avroDeserializer =
            AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build()
        avroDeserializer.avroRecordType = AvroRecordType.GENERIC_RECORD

        return avroDeserializer.deserialize(ByteBuffer.wrap(serializedData), schema)
    }

    /**
     * De-serializes the given byte array to an Object.
     *
     * @param data   data to de-serialize as byte array
     * @param schema schema for the data
     * @return de-serialized object
     */
    private fun deserialize(
        deserializer: GlueSchemaRegistryDataFormatDeserializer,
        data: ByteArray,
        schema: Schema,
    ): Any? = deserializer.deserialize(ByteBuffer.wrap(data), schema)

    /**
     * De-serializes the given ByteBuffer to an Object. Overload without accepting schema version id.
     *
     * @param buffer data to de-serialize as ByteBuffer
     * @param schema schema for the data
     * @return de-serialized object
     */
    private fun deserialize(
        deserializer: GlueSchemaRegistryDataFormatDeserializer,
        buffer: ByteBuffer,
        schema: Schema,
    ): Any? = deserializer.deserialize(buffer, schema)

    /**
     * De-serializes and asserts the de-serialized object.
     *
     * @param schema           Avro schema object
     * @param serializedObject serialized object for comparison
     * @param serializedData   serialized data bye array
     */
    private fun deserializeAndAssertGenericRecord(
        schema: Schema,
        serializedObject: Any,
        serializedData: ByteArray,
    ) {
        val deserializedObject = createDeserializedObjectForGenericRecord(schema, serializedData)
        assertTrue(serializedObject == deserializedObject)
    }

    /**
     * Helper method to create AvroDeserializer instance for given record type.
     *
     * @param recordType Generic or Specific record
     * @return AvroDeserializer instance
     */
    fun createAvroDeserializer(recordType: AvroRecordType): AvroDeserializer {
        val avroDeserializer =
            AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build()
        avroDeserializer.avroRecordType = recordType
        return avroDeserializer
    }

    /**
     * Tests creating Avro de-serializer instance and checks for config instance.
     */
    @Test
    fun testCreateAvroDeserializer_withOnlyConfigs_configsMatch() {
        val avroDeserializer =
            AvroDeserializer
                .builder()
                .configs(this.schemaRegistrySerDeConfigs)
                .build()

        assertEquals(this.schemaRegistrySerDeConfigs, avroDeserializer.schemaRegistrySerDeConfigs)
    }

    /**
     * Tests the de-serialization for exception case where data length is invalid.
     */
    @Test
    fun testDeserialize_incompleteData_throwsException() {
        val serializedData =
            byteArrayOf(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
            )

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD)
        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                avroDeserializer.deserialize(ByteBuffer.wrap(serializedData), schemaObject)
            }
        val rootCause = ex.cause
        assertTrue(rootCause is GlueSchemaRegistryIncompatibleDataException)
        assertEquals("Data is not compatible with schema registry size: 2", rootCause!!.message)
    }

    /**
     * Tests the de-serialization for exception case where the header version byte
     * is unknown.
     */
    @Test
    fun testDeserialize_invalidHeaderVersionByte_throwsException() {
        val serializedData =
            SerializedByteArrayGenerator.constructBasicSerializedByteBuffer(
                99,
                AWSSchemaRegistryConstants.COMPRESSION_BYTE,
                UUID.randomUUID(),
            )

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD)
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                avroDeserializer.deserialize(serializedData, schemaObject)
            }
        val rootCause = ex.cause
        assertTrue(rootCause is GlueSchemaRegistryIncompatibleDataException)
        assertEquals("Invalid schema registry header version byte in data", rootCause!!.message)
    }

    /**
     * Tests the de-serialization for exception case where the compression byte is
     * unknown.
     */
    @Test
    fun testDeserialize_invalidCompressionByte_throwsException() {
        val serializedData =
            SerializedByteArrayGenerator.constructBasicSerializedByteBuffer(
                AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
                99,
                UUID.randomUUID(),
            )

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD)

        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                avroDeserializer.deserialize(serializedData, schemaObject)
            }
        val rootCause = ex.cause
        assertTrue(rootCause is GlueSchemaRegistryIncompatibleDataException)
        assertEquals("Invalid schema registry compression byte in data", rootCause!!.message)
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_genericRecord_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()

        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)
        assertGenericRecord(genericRecord, deserializedObject)
        // Assert the instance is getting cached.
        assertEquals(1, avroDeserializer.getDatumReaderCache().size())
    }

    @Test
    fun testDeserialize_genericRecordWithReaderSchema_projectsOntoTheReaderSchema() {
        val genericRecord = RecordGenerator.createGenericAvroRecord()

        val serializedData =
            createBasicSerializedData(genericRecord, AWSSchemaRegistryConstants.COMPRESSION.NONE.name, DataFormat.AVRO)
        val writerSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val readerSchema = SchemaLoader.loadAvroSchema(AVRO_USER_READER_PROJECTION_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializerWithReaderSchema(readerSchema.toString())

        val schemaObject = Schema(writerSchema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)

        assertTrue(deserializedObject is GenericRecord)
        val deserializedRecord = deserializedObject as GenericRecord
        assertEquals(readerSchema, deserializedRecord.schema)
        assertEquals(1, deserializedRecord.schema.fields.size)
        assertEquals(Utf8("sansa"), deserializedRecord.get("name"))
        assertThrows(AvroRuntimeException::class.java) { deserializedRecord.get("favorite_number") }
    }

    @Test
    fun testDeserialize_genericRecordWithoutReaderSchema_keepsTheWriterSchema() {
        val genericRecord = RecordGenerator.createGenericAvroRecord()

        val serializedData =
            createBasicSerializedData(genericRecord, AWSSchemaRegistryConstants.COMPRESSION.NONE.name, DataFormat.AVRO)
        val writerSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val schemaObject = Schema(writerSchema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)

        assertEquals(writerSchema, (deserializedObject as GenericRecord).schema)
        assertEquals(genericRecord, deserializedObject)
    }

    @Test
    fun testDeserialize_readerSchemaIsPerDeserializerInstance_soTheDatumReaderCacheStaysValid() {
        val genericRecord = RecordGenerator.createGenericAvroRecord()

        val serializedData =
            createBasicSerializedData(genericRecord, AWSSchemaRegistryConstants.COMPRESSION.NONE.name, DataFormat.AVRO)
        val writerSchema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val readerSchema = SchemaLoader.loadAvroSchema(AVRO_USER_READER_PROJECTION_SCHEMA_FILE)
        val schemaObject = Schema(writerSchema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val projecting = createAvroDeserializerWithReaderSchema(readerSchema.toString())
        val plain = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val projected = projecting.deserialize(serializedData, schemaObject)
        val whole = plain.deserialize(serializedData, schemaObject)

        assertEquals(readerSchema, (projected as GenericRecord).schema)
        assertEquals(writerSchema, (whole as GenericRecord).schema)
        assertEquals(1, projecting.getDatumReaderCache().size())
        assertEquals(1, plain.getDatumReaderCache().size())
        assertTrue(projecting.getDatumReaderCache() !== plain.getDatumReaderCache())
    }

    private fun createAvroDeserializerWithReaderSchema(readerSchemaDefinition: String): AvroDeserializer {
        val readerConfigs = HashMap(configs)
        readerConfigs[AWSSchemaRegistryConstants.AVRO_READER_SCHEMA] = readerSchemaDefinition
        val avroDeserializer =
            AvroDeserializer
                .builder()
                .configs(GlueSchemaRegistryConfiguration(readerConfigs))
                .build()
        avroDeserializer.avroRecordType = AvroRecordType.GENERIC_RECORD
        return avroDeserializer
    }

    fun assertGenericRecord(
        genericRecord: GenericRecord,
        deserializedObject: Any?,
    ) {
        assertTrue(deserializedObject is GenericRecord)
        assertTrue(deserializedObject == genericRecord)
    }

    /**
     * Test whether the serialized generic record with specific record de-serializer
     * mode can be de-serialized back to a the user defined custom object and the
     * values are same between the generic record and custom object.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_genericRecordWithSpecificMode_equalsOriginal(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()

        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)

        // Assert the instance is getting cached.
        assertEquals(1, avroDeserializer.getDatumReaderCache().size())
        assertGenericRecordWithSpecificRecordMode(genericRecord, deserializedObject)
    }

    private fun assertGenericRecordWithSpecificRecordMode(
        genericRecord: GenericRecord,
        deserializedObject: Any?,
    ) {
        val deserializedUserObject = deserializedObject as User
        assertAll(
            "Deserialization is successful!",
            { assertNotNull(deserializedObject) },
            { assertEquals(genericRecord.get("name"), deserializedUserObject.name.toString()) },
            { assertEquals(genericRecord.get("favorite_number"), deserializedUserObject.favoriteNumber) },
            { assertEquals(genericRecord.get("favorite_color"), deserializedUserObject.favoriteColor.toString()) },
        )
    }

    /**
     * Test whether the serialized user defined custom object with specific record
     * de-serializer mode can be de-serialized back to a the user defined custom
     * object.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_specificRecord_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val userDefinedObject = RecordGenerator.createSpecificAvroRecord()
        val serializedData = createBasicSerializedData(userDefinedObject, compressionType.name, DataFormat.AVRO)

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.SPECIFIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)
        assertAll(
            "De-serialized object is User type and equals the serialized object",
            { assertTrue(deserializedObject is User) },
            { assertTrue(deserializedObject == userDefinedObject) },
        )
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_genericRecordWithoutSchemaVersionId_equalsOriginal(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()
        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val deserializedObject = deserialize(avroDeserializer, serializedData, schemaObject)

        assertGenericRecord(genericRecord, deserializedObject)
    }

    /**
     * Test whether the serialized generic record can be de-serialized back to the
     * generic record instance.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_genericRecordWithByteArray_equalsOriginal(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()
        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = deserialize(avroDeserializer, serializedData.array(), schemaObject)

        assertGenericRecord(genericRecord, deserializedObject)
    }

    /**
     * Test whether the serialized user defined custom object with generic record
     * de-serializer mode can be de-serialized back to a generic record object and
     * the values are same between t two custom objects.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_specificRecordInGenericMode_equalsOriginal(
        compressionType: AWSSchemaRegistryConstants.COMPRESSION,
    ) {
        val userDefinedObject = RecordGenerator.createSpecificAvroRecord()
        val serializedData = createBasicSerializedData(userDefinedObject, compressionType.name, DataFormat.AVRO)

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = avroDeserializer.deserialize(serializedData, schemaObject)

        assertSpecificRecordInGenericRecordMode(userDefinedObject, deserializedObject)
    }

    private fun assertSpecificRecordInGenericRecordMode(
        userDefinedObject: User,
        deserializedObject: Any?,
    ) {
        val deserializedGenericRecord = deserializedObject as GenericRecord
        assertAll(
            "Deserialization is successful!",
            { assertNotNull(deserializedObject) },
            { assertEquals(userDefinedObject.name, deserializedGenericRecord.get("name").toString()) },
            { assertEquals(userDefinedObject.favoriteNumber, userDefinedObject.get("favorite_number")) },
            { assertEquals(userDefinedObject.favoriteColor, userDefinedObject.get("favorite_color").toString()) },
        )
    }

    /**
     * Test whether serialized enum can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_enumSchema_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForEnum = SchemaLoader.loadAvroSchema(AVRO_USER_ENUM_SCHEMA_FILE)
        val enumSymbol = GenericData.EnumSymbol(schemaForEnum, "ONE")

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, enumSymbol, UUID.randomUUID())
        val schemaObject = Schema(schemaForEnum.toString(), DataFormat.AVRO.name, "testAvroSchema")
        deserializeAndAssertGenericRecord(schemaObject, enumSymbol, serializedData)
    }

    /**
     * Test whether serialized integer array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_integerArrays_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE)
        val array = GenericData.Array<Int>(1, schemaForArray)
        array.add(1)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID())

        val schemaObject = Schema(schemaForArray.toString(), DataFormat.AVRO.name, "testAvroSchema")
        deserializeAndAssertGenericRecord(schemaObject, array, serializedData)
    }

    /**
     * Test whether serialized object array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_objectArrays_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_SCHEMA_FILE)
        val array = GenericData.Array<Any>(1, schemaForArray)
        array.add(1)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID())

        val schemaObject = Schema(schemaForArray.toString(), DataFormat.AVRO.name, "testAvroSchema")

        deserializeAndAssertGenericRecord(schemaObject, array, serializedData)
    }

    /**
     * Test whether serialized union object can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_unions_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForUnion = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE)
        val unionRecord = GenericData.Record(schemaForUnion)
        unionRecord.put("experience", 1)
        unionRecord.put("age", 30)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, unionRecord, UUID.randomUUID())

        val schemaObject = Schema(schemaForUnion.toString(), DataFormat.AVRO.name, "testAvroSchema")

        deserializeAndAssertGenericRecord(schemaObject, unionRecord, serializedData)
    }

    /**
     * Test whether serialized union object with null value can be de-serialized
     * back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_unionsWithNull_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForUnion = SchemaLoader.loadAvroSchema(AVRO_USER_UNION_SCHEMA_FILE)
        val unionRecord = GenericData.Record(schemaForUnion)
        unionRecord.put("experience", null)
        unionRecord.put("age", 30)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, unionRecord, UUID.randomUUID())

        val schemaObject = Schema(schemaForUnion.toString(), DataFormat.AVRO.name, "testAvroSchema")

        deserializeAndAssertGenericRecord(schemaObject, unionRecord, serializedData)
    }

    /**
     * Test whether serialized fixed array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_fixedArray_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForFixedByteArray = SchemaLoader.loadAvroSchema(AVRO_USER_FIXED_SCHEMA_FILE)
        val fixedRecord = GenericData.Fixed(schemaForFixedByteArray)
        val bytes = "byte array".toByteArray()
        fixedRecord.bytes(bytes)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, fixedRecord, UUID.randomUUID())

        val schemaObject = Schema(schemaForFixedByteArray.toString(), DataFormat.AVRO.name, "testAvroSchema")

        deserializeAndAssertGenericRecord(schemaObject, fixedRecord, serializedData)
    }

    /**
     * Test whether serialized string array can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_stringArrays_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schemaForArray = SchemaLoader.loadAvroSchema(AVRO_USER_ARRAY_STRING_SCHEMA_FILE)
        val array = GenericData.Array<String>(1, schemaForArray)
        array.add("TestValue")

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, array, UUID.randomUUID())

        val schemaObject = Schema(schemaForArray.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = createDeserializedObjectForGenericRecord(schemaObject, serializedData)
        validateStringRecords(array, deserializedObject)
    }

    private fun validateStringRecords(
        array: GenericData.Array<String>,
        deserializedObject: Any?,
    ) {
        @Suppress("UNCHECKED_CAST")
        val actualValue =
            (deserializedObject as GenericData.Array<Utf8>)
                .get(0)
                .toString()
        assertEquals(array.get(0), actualValue)
    }

    /**
     * Test whether serialized map can be de-serialized back.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_maps_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val avroRecordMapName = "meta"
        val keyName = "testKey"
        val schemaForMap = SchemaLoader.loadAvroSchema(AVRO_USER_MAP_SCHEMA_FILE)
        val mapRecord = GenericData.Record(schemaForMap)
        val map = HashMap<String, Long>()
        map[keyName] = 1L
        mapRecord.put(avroRecordMapName, map)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(DataFormat.AVRO, mapRecord, UUID.randomUUID())

        val schemaObject = Schema(schemaForMap.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = createDeserializedObjectForGenericRecord(schemaObject, serializedData)
        validateEnumRecord(avroRecordMapName, keyName, map, deserializedObject)
    }

    private fun validateEnumRecord(
        avroRecordMapName: String,
        keyName: String,
        map: Map<String, Long>,
        deserializedObject: Any?,
    ) {
        @Suppress("UNCHECKED_CAST")
        val deserializedMap =
            (deserializedObject as GenericData.Record).get(avroRecordMapName) as HashMap<Utf8, Long>
        assertEquals(
            map.keys
                .iterator()
                .next(),
            deserializedMap.keys
                .iterator()
                .next()
                .toString(),
        )
        assertEquals(map[keyName], deserializedMap[Utf8(keyName)])
    }

    /**
     * Test for combination of types and check for de-serialized values.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_allTypes_equalsOriginal(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE)
        val avroRecordMapName = "meta"
        val keyName = "testKey"

        val enumSymbol = GenericData.EnumSymbol(schema, "ONE")
        val integerArrayList = ArrayList<Int>()
        integerArrayList.add(1)

        val genericRecordWithAllTypes = GenericData.Record(schema)
        val map = HashMap<String, Long>()
        map[keyName] = 1L
        genericRecordWithAllTypes.put("name", "Joe")
        genericRecordWithAllTypes.put("favorite_number", 1)
        genericRecordWithAllTypes.put(avroRecordMapName, map)
        genericRecordWithAllTypes.put("listOfColours", integerArrayList)
        genericRecordWithAllTypes.put("integerEnum", enumSymbol)

        val glueSchemaRegistrySerializationFacade = createGlueSchemaRegistryFacade(compressionType.name)
        val serializedData =
            glueSchemaRegistrySerializationFacade.serialize(
                DataFormat.AVRO,
                genericRecordWithAllTypes,
                UUID.randomUUID(),
            )

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")

        val deserializedObject = createDeserializedObjectForGenericRecord(schemaObject, serializedData)

        validateRecord(avroRecordMapName, keyName, enumSymbol, integerArrayList, map, deserializedObject)
    }

    private fun validateRecord(
        avroRecordMapName: String,
        keyName: String,
        enumSymbol: GenericData.EnumSymbol,
        integerArrayList: ArrayList<Int>,
        map: Map<String, Long>,
        deserializedObject: Any?,
    ) {
        val deserializedRecord = deserializedObject as GenericData.Record
        assertEquals("Joe", deserializedRecord.get("name").toString())
        assertEquals(1, deserializedRecord.get("favorite_number"))

        validateEnumRecord(avroRecordMapName, keyName, map, deserializedObject)

        assertEquals(integerArrayList, deserializedRecord.get("listOfColours"))

        assertEquals(enumSymbol, deserializedRecord.get("integerEnum"))
    }

    /**
     * Test invalid record type configuration.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_unknownRecordType_throwsException(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()
        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)

        val schema = SchemaLoader.loadAvroSchema(AVRO_USER_SCHEMA_FILE)
        val avroDeserializer = createAvroDeserializer(AvroRecordType.UNKNOWN)

        val schemaObject = Schema(schema.toString(), DataFormat.AVRO.name, "testAvroSchema")
        val ex =
            assertThrows(AWSSchemaRegistryException::class.java) {
                deserialize(avroDeserializer, serializedData.array(), schemaObject)
            }
        val rootCause = ex.cause!!.cause
        assertTrue(rootCause is UnsupportedOperationException)
        assertEquals("Unsupported AvroRecordType: UNKNOWN", rootCause!!.message)
    }

    /**
     * Tests the de-serialization for Schema parse errors by simulating
     * SchemaParseException which will be wrapper under AWSSchemaRegistryException
     * for invalid schemas.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION::class)
    fun testDeserialize_invalidSchema_throwsException(compressionType: AWSSchemaRegistryConstants.COMPRESSION) {
        val genericRecord = RecordGenerator.createGenericAvroRecord()
        val serializedData = createBasicSerializedData(genericRecord, compressionType.name, DataFormat.AVRO)

        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        assertThrows(AWSSchemaRegistryException::class.java) {
            avroDeserializer.deserialize(serializedData, Schema("Invalid", DataFormat.AVRO.name, "invalidName"))
        }
    }

    /**
     * Test deserialize for null pointer exception by passing null byte data.
     */
    @Test
    fun testDeserialize_nullData_throwsException() {
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)
        val schemaObject = Schema("Test", DataFormat.AVRO.name, "testAvroSchema")
        assertThrows(NullPointerException::class.java) {
            deserialize(avroDeserializer, nullOf<ByteArray>(), schemaObject)
        }
    }

    /**
     * Test deserialize for null pointer exception by passing null buffer .
     */
    @Test
    fun testDeserialize_nullByteBuffer_throwsException() {
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)
        val schemaObject = Schema("Test", DataFormat.AVRO.name, "testAvroSchema")
        assertThrows(NullPointerException::class.java) {
            deserialize(avroDeserializer, nullOf<ByteBuffer>(), schemaObject)
        }
    }

    /**
     * Test deserialize for null pointer exception by passing data and null schema.
     */
    @Test
    fun testDeserialize_nullSchemaWithData_throwsException() {
        val serializedData = getTestSerializedByteData()
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        assertThrows(NullPointerException::class.java) {
            deserialize(avroDeserializer, serializedData, nullOf())
        }
    }

    /**
     * Test deserialize for null pointer exception by passing buffer and null schema.
     */
    @Test
    fun testDeserialize_nullSchemaWithBuffer_throwsException() {
        val serializedByteBuffer = getTestSerializedByteBufferData()
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        assertThrows(NullPointerException::class.java) {
            deserialize(avroDeserializer, serializedByteBuffer, nullOf())
        }
    }

    /**
     * Test deserialize for null pointer exception by passing schemaVersionId, schema and null buffer.
     */
    @Test
    fun testDeserialize_withSchemaVersionIdWithNullBufferWithSchema_throwsException() {
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)
        assertThrows(NullPointerException::class.java) {
            avroDeserializer.deserialize(nullOf(), Schema("Test", DataFormat.AVRO.name, "test"))
        }
    }

    /**
     * Test deserialize for null pointer exception by passing schemaVersionId, buffer and null schema.
     */
    @Test
    fun testDeserialize_withSchemaVersionIdWithBufferWithNullSchema_throwsException() {
        val serializedByteBuffer = getTestSerializedByteBufferData()
        val avroDeserializer = createAvroDeserializer(AvroRecordType.GENERIC_RECORD)

        assertThrows(NullPointerException::class.java) {
            avroDeserializer.deserialize(serializedByteBuffer, nullOf())
        }
    }

    /**
     * Helper method to get SerializedByteBuffer Data for test
     */
    fun getTestSerializedByteBufferData(): ByteBuffer = SerializedByteArrayGenerator.constructBasicSerializedByteBuffer(
        99,
        AWSSchemaRegistryConstants.COMPRESSION_BYTE,
        UUID.randomUUID(),
    )

    /**
     * Helper method to get SerializedByte Data for test
     */
    fun getTestSerializedByteData(): ByteArray = byteArrayOf(
        AWSSchemaRegistryConstants.HEADER_VERSION_BYTE,
        AWSSchemaRegistryConstants.COMPRESSION_BYTE,
    )

    companion object {
        const val AVRO_USER_SCHEMA_FILE = "src/test/resources/avro/user.avsc"
        const val AVRO_USER_READER_PROJECTION_SCHEMA_FILE = "src/test/resources/avro/user_reader_projection.avsc"
        const val AVRO_USER_ENUM_SCHEMA_FILE = "src/test/resources/avro/user_enum.avsc"
        const val AVRO_USER_ARRAY_SCHEMA_FILE = "src/test/resources/avro/user_array.avsc"
        const val AVRO_USER_UNION_SCHEMA_FILE = "src/test/resources/avro/user_union.avsc"
        const val AVRO_USER_FIXED_SCHEMA_FILE = "src/test/resources/avro/user_fixed.avsc"
        const val AVRO_USER_ARRAY_STRING_SCHEMA_FILE = "src/test/resources/avro/user_array_String.avsc"
        const val AVRO_USER_MAP_SCHEMA_FILE = "src/test/resources/avro/user_map.avsc"
        const val AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/resources/avro/user3.avsc"
        private val TEST_GENERIC_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63")
    }
}
