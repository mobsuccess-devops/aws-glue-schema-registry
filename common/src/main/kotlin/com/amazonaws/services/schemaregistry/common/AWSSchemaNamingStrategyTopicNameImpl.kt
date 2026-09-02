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

private const val KEY_SUFFIX = "-key"
private const val VALUE_SUFFIX = "-value"

/**
 * Names a schema after the transport it travels on and the side of the record it describes:
 * `<topic>-key` for a key, `<topic>-value` for a value. This is the Confluent
 * `TopicNameStrategy` naming, and it is what keeps a topic's key schema and value schema from
 * sharing one registry entry.
 *
 * Selected with `schemaNameGenerationClass`; it is never the default.
 */
class AWSSchemaNamingStrategyTopicNameImpl : AWSSchemaNamingStrategy {
    override fun getSchemaName(transportName: String?): String? = suffixed(transportName, VALUE_SUFFIX)

    override fun getSchemaName(
        transportName: String?,
        data: Any?,
        isKey: Boolean,
    ): String? = suffixed(transportName, if (isKey) KEY_SUFFIX else VALUE_SUFFIX)

    private fun suffixed(
        transportName: String?,
        suffix: String,
    ): String? = transportName?.let { it + suffix }
}
