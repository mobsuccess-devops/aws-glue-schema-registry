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

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import org.apache.avro.Schema
import org.apache.avro.SchemaValidationException
import org.apache.avro.SchemaValidator
import org.apache.avro.SchemaValidatorBuilder
import org.apache.avro.Schemas
import org.slf4j.LoggerFactory
import java.util.Collections
import java.util.Objects

class AvroSchema : ParsedSchema {
    private val schemaObj: Schema?
    private var canonicalString: String? = null
    private val version: Int?
    private val resolvedReferences: Map<String, String>
    private val isNew: Boolean

    @JvmOverloads
    constructor(
        schemaString: String,
        resolvedReferences: Map<String, String> = emptyMap(),
        version: Int? = null,
        isNew: Boolean = false,
    ) {
        this.isNew = isNew
        val parser = getParser()
        for (schema in resolvedReferences.values) {
            parser.parse(schema)
        }
        schemaObj = parser.parse(schemaString)
        this.resolvedReferences = Collections.unmodifiableMap(resolvedReferences)
        this.version = version
    }

    @JvmOverloads
    constructor(schemaObj: Schema?, version: Int? = null) {
        isNew = false
        this.schemaObj = schemaObj
        resolvedReferences = emptyMap()
        this.version = version
    }

    private constructor(
        schemaObj: Schema?,
        canonicalString: String?,
        resolvedReferences: Map<String, String>,
        version: Int?,
        isNew: Boolean,
    ) {
        this.isNew = isNew
        this.schemaObj = schemaObj
        this.canonicalString = canonicalString
        this.resolvedReferences = resolvedReferences
        this.version = version
    }

    fun copy(): AvroSchema = AvroSchema(schemaObj, canonicalString, resolvedReferences, version, isNew)

    protected fun getParser(): Schema.Parser {
        val parser = Schema.Parser()
        parser.setValidateDefaults(isNew())
        return parser
    }

    override fun rawSchema(): Schema? = schemaObj

    override fun schemaType(): String = TYPE

    override fun name(): String? = if (schemaObj != null && schemaObj.type == Schema.Type.RECORD) schemaObj.fullName else null

    override fun canonicalString(): String {
        if (schemaObj == null) {
            return null!!
        }
        if (canonicalString == null) {
            val parser = getParser()
            val schemaRefs = resolvedReferences.values.map { parser.parse(it) }
            canonicalString = Schemas.toString(schemaObj, schemaRefs)
        }
        return canonicalString!!
    }

    fun version(): Int? = version

    fun resolvedReferences(): Map<String, String> = resolvedReferences

    fun isNew(): Boolean = isNew

    override fun isBackwardCompatible(previousSchema: ParsedSchema): List<String> {
        if (schemaType() != previousSchema.schemaType()) {
            return Collections.singletonList("Incompatible because of different schema type")
        }
        try {
            BACKWARD_VALIDATOR.validate(schemaObj, Collections.singleton((previousSchema as AvroSchema).schemaObj))
            return emptyList()
        } catch (e: SchemaValidationException) {
            // Cast rather than defaulting: the original propagated a null message as-is.
            @Suppress("UNCHECKED_CAST")
            return Collections.singletonList(e.message) as List<String>
        } catch (e: Exception) {
            log.error("Unexpected exception during compatibility check", e)
            return Collections.singletonList("Unexpected exception during compatibility check: ${e.message}")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as AvroSchema
        return schemaObj == that.schemaObj && version == that.version
    }

    override fun hashCode(): Int = Objects.hash(schemaObj, version)

    override fun toString(): String = canonicalString()

    companion object {
        private val log = LoggerFactory.getLogger(AvroSchema::class.java)

        const val TYPE = "AVRO"

        private val BACKWARD_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().canReadStrategy().validateLatest()
    }
}
