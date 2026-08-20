package com.amazonaws.services.schemaregistry.deserializers

import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import java.nio.ByteBuffer
import java.util.UUID

@ExtendWith(MockitoExtension::class)
class GlueSchemaRegistryDeserializerImplTest {
    @Mock
    private lateinit var credentialsProvider: AwsCredentialsProvider

    @Mock
    private lateinit var glueSchemaRegistryDeserializationFacade: GlueSchemaRegistryDeserializationFacade

    private val config: Map<String, Any> = mapOf(AWSSchemaRegistryConstants.AWS_REGION to REGION)

    private lateinit var glueSchemaRegistryDeserializer: GlueSchemaRegistryDeserializer

    @BeforeEach
    fun setUp() {
        glueSchemaRegistryDeserializer =
            GlueSchemaRegistryDeserializerImpl(glueSchemaRegistryDeserializationFacade)
    }

    @Test
    fun instantiate_WithConfig_CreatesInstance() {
        val configuration = GlueSchemaRegistryConfiguration(REGION)
        val glueSchemaRegistryDeserializer: GlueSchemaRegistryDeserializer =
            GlueSchemaRegistryDeserializerImpl(credentialsProvider, configuration)

        assertNotNull(glueSchemaRegistryDeserializer)
    }

    @Test
    fun getData_WhenValidSchemaRegistryEncodedBytesAreSent_ReturnsActualData() {
        val expected = byteArrayOf(12, 83, 82)
        doReturn(expected)
            .whenever(glueSchemaRegistryDeserializationFacade)
            .getActualData(ENCODED_DATA)

        val actual = glueSchemaRegistryDeserializer.getData(ENCODED_DATA)

        assertEquals(expected, actual)
    }

    @Test
    fun getSchema_WhenValidSchemaRegistryEncodedBytesAreSent_ReturnsSchema() {
        doReturn(SCHEMA_REGISTRY_SCHEMA)
            .whenever(glueSchemaRegistryDeserializationFacade)
            .getSchema(ENCODED_DATA)

        val actual = glueSchemaRegistryDeserializer.getSchema(ENCODED_DATA)

        assertEquals(SCHEMA_REGISTRY_SCHEMA, actual)
    }

    @Test
    fun canDeserialize_WhenInvalidSchemaRegistryEncodedDataIsSent_ReturnsFalse() {
        assertFalse(glueSchemaRegistryDeserializer.canDeserialize(ENCODED_DATA))
    }

    @Test
    fun canDeserialize_WhenValidSchemaRegistryEncodedDataIsSent_ReturnsTrue() {
        val encodedMessage = constructValidSerializedData()
        doReturn(true)
            .whenever(glueSchemaRegistryDeserializationFacade)
            .canDeserialize(ENCODED_DATA)

        assertTrue(glueSchemaRegistryDeserializer.canDeserialize(ENCODED_DATA))
    }

    private fun constructValidSerializedData(): ByteArray {
        val byteBuffer = ByteBuffer.wrap(ByteArray(18))
        val uuid = UUID.randomUUID()
        byteBuffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE)
        byteBuffer.put(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE)
        byteBuffer.putLong(uuid.mostSignificantBits)
        byteBuffer.putLong(uuid.leastSignificantBits)

        return byteBuffer.array()
    }

    companion object {
        private const val REGION = "us-west-2"
        private val ENCODED_DATA = byteArrayOf(8, 9, 12, 83, 82)
        private val SCHEMA_REGISTRY_SCHEMA = Schema("{}", "AVRO", "schemaFoo")
    }
}
