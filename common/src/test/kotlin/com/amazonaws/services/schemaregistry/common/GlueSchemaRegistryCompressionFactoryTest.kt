package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GlueSchemaRegistryCompressionFactoryTest {
    private val configs: MutableMap<String, Any> = HashMap()
    private lateinit var glueSchemaRegistryConfiguration: GlueSchemaRegistryConfiguration
    lateinit var glueSchemaRegistryCompressionFactory: GlueSchemaRegistryCompressionFactory

    @BeforeEach
    fun setup() {
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "https://test"
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.REGISTRY_NAME] = "User-Registry"
        glueSchemaRegistryConfiguration = GlueSchemaRegistryConfiguration(configs)
        glueSchemaRegistryCompressionFactory = GlueSchemaRegistryCompressionFactory()
    }

    @Test
    fun testConstructor_validSerdeConfigs_succeeds() {
        assertDoesNotThrow { GlueSchemaRegistryCompressionFactory() }
    }

    @Test
    fun testGetCompressionHandler_nullCompressionType_returnsNull() {
        assertNull(glueSchemaRegistryCompressionFactory.getCompressionHandler(null))
    }

    @Test
    fun testGetCompressionHandler_noneCompressionType_returnsNull() {
        assertNull(
            glueSchemaRegistryCompressionFactory.getCompressionHandler(
                AWSSchemaRegistryConstants.COMPRESSION.NONE,
            ),
        )
    }

    @Test
    fun testGetCompressionHandler_zlibCompressionTypeWithZLibCompressionNotInitialized_initializesUsingDefaultCompression() {
        val glueSchemaRegistryCompressionHandler =
            glueSchemaRegistryCompressionFactory.getCompressionHandler(
                AWSSchemaRegistryConstants.COMPRESSION.ZLIB,
            )
        assertEquals(GlueSchemaRegistryDefaultCompression::class.java, glueSchemaRegistryCompressionHandler!!.javaClass)
    }

    @Test
    fun testGetCompressionHandler_zlibCompressionTypeWithZLibCompressionInitialized_initializesUsingDefaultCompression() {
        // Initialize call
        val instance1 =
            glueSchemaRegistryCompressionFactory.getCompressionHandler(
                AWSSchemaRegistryConstants.COMPRESSION.ZLIB,
            )
        // Return initialized instance.
        val instance2 =
            glueSchemaRegistryCompressionFactory.getCompressionHandler(
                AWSSchemaRegistryConstants.COMPRESSION.ZLIB,
            )
        assertEquals(instance1, instance2)
    }

    @Test
    fun testGetCompressionHandler_unknownCompressionByte_returnsNull() {
        val compressionBytes = "1".toByte()
        assertNull(glueSchemaRegistryCompressionFactory.getCompressionHandler(compressionBytes))
    }

    @Test
    fun testGetCompressionHandler_knownCompressionByte_returnsNull() {
        assertEquals(
            GlueSchemaRegistryDefaultCompression::class.java,
            glueSchemaRegistryCompressionFactory
                .getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION_BYTE)!!
                .javaClass,
        )
    }
}
