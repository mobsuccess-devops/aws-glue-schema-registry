package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class GlueSchemaRegistryDefaultCompressionTest {
    private val configs: MutableMap<String, Any> = HashMap()
    private lateinit var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration
    private lateinit var glueSchemaRegistryDefaultCompression: GlueSchemaRegistryDefaultCompression

    @BeforeEach
    fun setup() {
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.REGISTRY_NAME] = "User-Topic"
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        glueSchemaRegistryDefaultCompression = GlueSchemaRegistryDefaultCompression()
    }

    @Test
    fun testCompress_invalidInput_throwsAWSSchemaRegistryException() {
        try {
            glueSchemaRegistryDefaultCompression.compress(null)
            fail<Unit>("No exception was thrown.")
        } catch (e: Exception) {
            assertEquals(AWSSchemaRegistryException::class.java, e.javaClass)
            assertEquals("Error while compressing data", e.message)
        }
    }

    @Test
    fun testCompress_byteArray_throwsAWSSchemaRegistryException() {
        assertDoesNotThrow { glueSchemaRegistryDefaultCompression.compress(TEST_BYTE_ARRAY) }
    }

    @Test
    fun testDecompress_invalidInput_throwsAWSSchemaRegistryException() {
        try {
            glueSchemaRegistryDefaultCompression.decompress(TEST_BYTE_ARRAY, 0, TEST_BYTE_ARRAY.size)
            fail<Unit>("No exception was thrown.")
        } catch (e: Exception) {
            assertEquals(AWSSchemaRegistryException::class.java, e.javaClass)
            assertEquals("Error while decompressing data", e.message)
        }
    }

    @Test
    fun testDecompress_validInput_throwsAWSSchemaRegistryException() {
        val compressedRecord = glueSchemaRegistryDefaultCompression.compress(TEST_BYTE_ARRAY)
        assertDoesNotThrow {
            glueSchemaRegistryDefaultCompression.decompress(compressedRecord, 0, compressedRecord.size)
        }
    }

    @Test
    fun testDecompress_truncatedInput_throwsAWSSchemaRegistryException() {
        val compressedRecord = glueSchemaRegistryDefaultCompression.compress(TEST_COMPRESSIBLE_BYTE_ARRAY)
        val truncatedRecord = compressedRecord.copyOf(compressedRecord.size / 2)

        val exception =
            assertThrows(AWSSchemaRegistryException::class.java) {
                assertTimeoutPreemptively(Duration.ofSeconds(10)) {
                    glueSchemaRegistryDefaultCompression.decompress(truncatedRecord, 0, truncatedRecord.size)
                }
            }
        val cause = exception.cause

        assertEquals("Error while decompressing data", exception.message)
        assertEquals(AWSSchemaRegistryException::class.java, cause?.javaClass)
        assertEquals(
            "Compressed bytes are truncated: the stream ends before the decompressed data does",
            cause?.message,
        )
    }

    companion object {
        private val TEST_BYTE_ARRAY = byteArrayOf(1, 2, 3)
        private val TEST_COMPRESSIBLE_BYTE_ARRAY = ByteArray(8192) { (it % 251).toByte() }
    }
}
