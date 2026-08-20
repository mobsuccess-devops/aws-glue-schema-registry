package com.amazonaws.services.schemaregistry.utils

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class GlueSchemaRegistryUtilsTest {
    private lateinit var glueSchemaRegistryUtils: GlueSchemaRegistryUtils
    private val configs: MutableMap<String, Any> = HashMap()

    @BeforeEach
    fun setup() {
        glueSchemaRegistryUtils = GlueSchemaRegistryUtils.getInstance()
        configs[AWSSchemaRegistryConstants.AWS_REGION] = "us-west-2"
        configs[AWSSchemaRegistryConstants.AWS_ENDPOINT] = "http://test"
        configs[AWSSchemaRegistryConstants.SCHEMA_NAME] = "User-Topic"
        configs[AWSSchemaRegistryConstants.DATA_FORMAT] = "json"
    }

    @Test
    fun testCheckIfPresentInMap_nullMap_throwsException() {
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryUtils.checkIfPresentInMap(nullOf(), "test-key")
        }
    }

    @Test
    fun testCheckIfPresentInMap_nullKey_throwsException() {
        assertThrows(NullPointerException::class.java) {
            glueSchemaRegistryUtils.checkIfPresentInMap(configs, nullOf())
        }
    }

    @Test
    fun testConfigureSchemaNamingStrategy_configMapWithoutSchemaGenerationClass_succeeds() {
        assertDoesNotThrow { glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs) }
    }

    @Test
    fun testConfigureSchemaNamingStrategy_configWithValidSchemaGenerationClass_succeeds() {
        configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
            "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy"
        assertDoesNotThrow { glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs) }
    }

    @Test
    fun testConfigureSchemaNamingStrategy_configWithInvalidSchemaGenerationClass_returnsNull() {
        configs[AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS] =
            "com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtilsTest"
        assertNull(glueSchemaRegistryUtils.configureSchemaNamingStrategy(configs))
    }

    @Test
    fun testGetSchemaName_configWithoutSchemaName_schemaNameMatches() {
        val expectedSchemaName = configs[AWSSchemaRegistryConstants.SCHEMA_NAME].toString()
        assertEquals(expectedSchemaName, glueSchemaRegistryUtils.getSchemaName(configs))
    }

    @Test
    fun testGetSchemaName_configWithoutSchemaName_returnsNull() {
        configs.remove(AWSSchemaRegistryConstants.SCHEMA_NAME)
        assertNull(glueSchemaRegistryUtils.getSchemaName(configs))
    }

    @Test
    fun testGetDataFormat_configWithDataFormat_schemaNameMatches() {
        val expectedDataFormat =
            configs[AWSSchemaRegistryConstants.DATA_FORMAT]
                .toString()
                .uppercase()
        assertEquals(expectedDataFormat, glueSchemaRegistryUtils.getDataFormat(configs))
    }

    @Test
    fun testGetDataFormat_configWithoutDataFormat_returnsNull() {
        configs.remove(AWSSchemaRegistryConstants.DATA_FORMAT)
        assertThrows(AWSSchemaRegistryException::class.java) { glueSchemaRegistryUtils.getDataFormat(configs) }
    }

    @Test
    fun testUseCustomerProvidedStrategyMethod_validClassName_succeeds() {
        val useCustomerProvidedStrategyMethod =
            GlueSchemaRegistryUtils::class.java.getDeclaredMethod("useCustomerProvidedStrategy", String::class.java)
        useCustomerProvidedStrategyMethod.isAccessible = true
        assertDoesNotThrow {
            useCustomerProvidedStrategyMethod.invoke(
                glueSchemaRegistryUtils,
                "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy",
            )
        }
    }

    @Test
    fun testUseCustomerProvidedStrategyMethod_invalidClassName_throwsException() {
        val useCustomerProvidedStrategyMethod =
            GlueSchemaRegistryUtils::class.java.getDeclaredMethod("useCustomerProvidedStrategy", String::class.java)
        useCustomerProvidedStrategyMethod.isAccessible = true
        try {
            useCustomerProvidedStrategyMethod.invoke(glueSchemaRegistryUtils, "test")
        } catch (e: Exception) {
            assertEquals(AWSSchemaRegistryException::class.java, e.cause!!.javaClass)
        }
    }

    @Test
    fun testUseCustomerProvidedStrategyMethod_nullClassName_throwsException() {
        val useCustomerProvidedStrategyMethod =
            GlueSchemaRegistryUtils::class.java.getDeclaredMethod("useCustomerProvidedStrategy", String::class.java)
        useCustomerProvidedStrategyMethod.isAccessible = true
        try {
            // Java read invoke(obj, null) as a null varargs array, that is: no argument at all
            useCustomerProvidedStrategyMethod.invoke(glueSchemaRegistryUtils)
        } catch (e: Exception) {
            assertEquals(IllegalArgumentException::class.java, e.javaClass)
        }
    }

    @Test
    fun testInitializeStrategyStrategyMethod_validClassName_succeeds() {
        val initializeStrategyMethod =
            GlueSchemaRegistryUtils::class.java.getDeclaredMethod("initializeStrategy", String::class.java)
        initializeStrategyMethod.isAccessible = true
        assertDoesNotThrow {
            initializeStrategyMethod.invoke(
                glueSchemaRegistryUtils,
                "com.amazonaws.services.schemaregistry.utils.external.CustomNamingStrategy",
            )
        }
    }

    @Test
    fun testInitializeStrategyStrategyMethod_nullClassName_throwsException() {
        val initializeStrategyMethod =
            GlueSchemaRegistryUtils::class.java.getDeclaredMethod("initializeStrategy", String::class.java)
        initializeStrategyMethod.isAccessible = true
        try {
            // Java read invoke(obj, null) as a null varargs array, that is: no argument at all
            initializeStrategyMethod.invoke(glueSchemaRegistryUtils)
        } catch (e: Exception) {
            assertEquals(IllegalArgumentException::class.java, e.javaClass)
        }
    }
}
