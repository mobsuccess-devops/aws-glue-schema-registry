/*
 * Copyright 2026 Mobsuccess.
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

package com.amazonaws.services.schemaregistry.common

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class AWSSchemaNamingStrategyTopicNameImplTest {
    private val strategy = AWSSchemaNamingStrategyTopicNameImpl()

    @Test
    fun testGetSchemaName_key_isSuffixedWithKey() {
        assertEquals("orders-key", strategy.getSchemaName("orders", RECORD, true))
    }

    @Test
    fun testGetSchemaName_value_isSuffixedWithValue() {
        assertEquals("orders-value", strategy.getSchemaName("orders", RECORD, false))
    }

    @Test
    fun testGetSchemaName_keyAndValueOfOneTopic_differ() {
        assertNotEquals(
            strategy.getSchemaName("orders", RECORD, true),
            strategy.getSchemaName("orders", RECORD, false),
        )
    }

    @Test
    fun testGetSchemaName_withoutASide_isTheValueName() {
        assertEquals("orders-value", strategy.getSchemaName("orders"))
        assertEquals("orders-value", strategy.getSchemaName("orders", RECORD))
    }

    @Test
    fun testGetSchemaName_nullTransportName_isNull() {
        assertNull(strategy.getSchemaName(null))
        assertNull(strategy.getSchemaName(null, RECORD))
        assertNull(strategy.getSchemaName(null, RECORD, true))
        assertNull(strategy.getSchemaName(null, RECORD, false))
    }

    @Test
    fun testGetSchemaName_dataIsNotRead() {
        assertEquals(strategy.getSchemaName("orders", RECORD, false), strategy.getSchemaName("orders", null, false))
    }

    @Test
    fun testConfigureSchemaNamingStrategy_selectedByTheExistingKey() {
        val configs =
            mapOf<String, Any>(
                AWSSchemaRegistryConstants.AWS_REGION to "us-west-2",
                AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS to
                    AWSSchemaNamingStrategyTopicNameImpl::class.java.name,
            )

        val configured = GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs)

        assertInstanceOf(AWSSchemaNamingStrategyTopicNameImpl::class.java, configured)
        assertEquals("orders-key", configured!!.getSchemaName("orders", RECORD, true))
    }

    @Test
    fun testConfigureSchemaNamingStrategy_defaultIsUnchanged() {
        val configs = mapOf<String, Any>(AWSSchemaRegistryConstants.AWS_REGION to "us-west-2")

        val configured = GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs)

        assertInstanceOf(AWSSchemaNamingStrategyDefaultImpl::class.java, configured)
        assertEquals("orders", configured!!.getSchemaName("orders", RECORD, true))
        assertEquals("orders", configured.getSchemaName("orders", RECORD, false))
    }

    companion object {
        private const val RECORD = "a record"
    }
}
