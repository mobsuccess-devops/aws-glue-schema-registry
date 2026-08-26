/*
 * Copyright 2019 Confluent Inc.
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.avro.JsonProperties
import org.apache.avro.LogicalTypes
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol
import org.apache.avro.generic.GenericFixed
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.internal.JacksonUtils
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Time
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.errors.DataException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.IdentityHashMap
import java.util.Objects
import org.apache.avro.Schema as ApacheAvroSchema
import org.apache.avro.SchemaBuilder as ApacheAvroSchemaBuilder

/**
 * Utilities for converting between our runtime data format and Avro, and (de)serializing that data.
 */
@Suppress("UNCHECKED_CAST", "UNUSED_VARIABLE")
open class AvroData(avroDataConfig: AvroDataConfig) {
    private val fromConnectSchemaCache: Cache<Schema, ApacheAvroSchema> =
        SynchronizedCache(LRUCache(avroDataConfig.getSchemasCacheSize()))
    private val toConnectSchemaCache: Cache<AvroSchema, Schema> =
        SynchronizedCache(LRUCache(avroDataConfig.getSchemasCacheSize()))
    private val connectMetaData: Boolean = avroDataConfig.isConnectMetaData()
    private val enhancedSchemaSupport: Boolean = avroDataConfig.isEnhancedAvroSchemaSupport()

    constructor(cacheSize: Int) : this(
        AvroDataConfig
            .Builder()
            .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize)
            .build(),
    )

    /**
     * Convert this object, in Connect data format, into an Avro object.
     */
    open fun fromConnectData(
        schema: Schema?,
        value: Any?,
    ): Any? {
        val avroSchema = fromConnectSchema(schema)
        return fromConnectData(schema, avroSchema, value)
    }

    protected open fun fromConnectData(
        schema: Schema?,
        avroSchema: ApacheAvroSchema,
        value: Any?,
    ): Any? = fromConnectData(schema, avroSchema, value, true, false, enhancedSchemaSupport)

    open fun fromConnectSchema(schema: Schema?): ApacheAvroSchema = fromConnectSchema(schema, HashMap<Schema, ApacheAvroSchema>())

    open fun fromConnectSchema(
        schema: Schema?,
        schemaMap: MutableMap<Schema, ApacheAvroSchema>,
    ): ApacheAvroSchema {
        val fromConnectContext = FromConnectContext(schemaMap)
        return fromConnectSchema(schema, fromConnectContext, false)
    }

    /**
     * SchemaMap is a map of already resolved internal schemas, this avoids type re-declaration if a
     * type is reused, this actually blows up if you don't do this and have a type used in multiple
     * places.
     *
     * Also it only holds reference the non-optional schemas as technically an optional is
     * actually a union of null and the non-opitonal, which if used in multiple places some optional
     * some non-optional will cause error as you redefine type.
     *
     * This is different to the global schema cache which is used to hold/cache fully resolved
     * schemas used to avoid re-resolving when presented with the same source schema.
     */
    open fun fromConnectSchema(
        schema: Schema?,
        fromConnectContext: FromConnectContext,
        ignoreOptional: Boolean,
    ): ApacheAvroSchema {
        if (schema == null) {
            return ANYTHING_SCHEMA
        }

        var cached = fromConnectSchemaCache.get(schema)

        if (cached == null && AVRO_TYPE_UNION != schema.name() && !schema.isOptional) {
            cached = fromConnectContext.schemaMap[schema]
        }
        if (cached != null) {
            return cached
        }

        var namespace: String? = NAMESPACE
        var name: String? = DEFAULT_SCHEMA_NAME
        if (schema.name() != null) {
            val split = splitName(schema.name())
            namespace = split[0]
            name = split[1]
        }

        // Extra type annotation information for otherwise lossy conversions
        var connectType: String? = null

        val baseSchema: ApacheAvroSchema =
            when (schema.type()) {
                Schema.Type.INT8 -> {
                    connectType = CONNECT_TYPE_INT8
                    ApacheAvroSchemaBuilder.builder().intType()
                }

                Schema.Type.INT16 -> {
                    connectType = CONNECT_TYPE_INT16
                    ApacheAvroSchemaBuilder.builder().intType()
                }

                Schema.Type.INT32 -> ApacheAvroSchemaBuilder.builder().intType()
                Schema.Type.INT64 -> ApacheAvroSchemaBuilder.builder().longType()
                Schema.Type.FLOAT32 -> ApacheAvroSchemaBuilder.builder().floatType()
                Schema.Type.FLOAT64 -> ApacheAvroSchemaBuilder.builder().doubleType()
                Schema.Type.BOOLEAN -> ApacheAvroSchemaBuilder.builder().booleanType()

                Schema.Type.STRING ->
                    if (enhancedSchemaSupport &&
                        schema.parameters() != null &&
                        schema.parameters().containsKey(AVRO_TYPE_ENUM)
                    ) {
                        val symbols: MutableList<String> = ArrayList()
                        for (entry in schema.parameters().entries) {
                            if (entry.key.startsWith("$AVRO_TYPE_ENUM.")) {
                                symbols.add(entry.value)
                            }
                        }
                        val enumDoc = schema.parameters()[AVRO_ENUM_DOC_PREFIX_PROP + name]
                        val enumDefault = schema.parameters()[AVRO_ENUM_DEFAULT_PREFIX_PROP + name]
                        ApacheAvroSchemaBuilder
                            .builder()
                            .enumeration(schema.parameters()[AVRO_TYPE_ENUM])
                            .doc(enumDoc)
                            .defaultSymbol(enumDefault)
                            .symbols(*symbols.toTypedArray())
                    } else {
                        ApacheAvroSchemaBuilder.builder().stringType()
                    }

                Schema.Type.BYTES -> {
                    val bytesSchema = ApacheAvroSchemaBuilder.builder().bytesType()
                    if (Decimal.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                        val scale = schema.parameters()[Decimal.SCALE_FIELD]!!.toInt()
                        bytesSchema.addProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP, IntNode(scale))
                        if (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
                            val precisionValue = schema.parameters()[CONNECT_AVRO_DECIMAL_PRECISION_PROP]
                            val precision = precisionValue!!.toInt()
                            bytesSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP, IntNode(precision))
                        } else {
                            bytesSchema.addProp(
                                AVRO_LOGICAL_DECIMAL_PRECISION_PROP,
                                IntNode(CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT),
                            )
                        }
                    }
                    bytesSchema
                }

                Schema.Type.ARRAY ->
                    ApacheAvroSchemaBuilder
                        .builder()
                        .array()
                        .items(fromConnectSchemaWithCycle(schema.valueSchema(), fromConnectContext, false))

                Schema.Type.MAP ->
                    // Avro only supports string keys, so we match the representation when possible, but
                    // otherwise fall back on a record representation
                    if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional) {
                        ApacheAvroSchemaBuilder
                            .builder()
                            .map()
                            .values(fromConnectSchemaWithCycle(schema.valueSchema(), fromConnectContext, false))
                    } else {
                        // Special record name indicates format
                        val fields: MutableList<ApacheAvroSchema.Field> = ArrayList()
                        val mapSchema: ApacheAvroSchema
                        if (schema.name() == null) {
                            mapSchema =
                                ApacheAvroSchema.createRecord(
                                    MAP_ENTRY_TYPE_NAME,
                                    null,
                                    namespace,
                                    false,
                                )
                        } else {
                            mapSchema = ApacheAvroSchema.createRecord(name, null, namespace, false)
                            mapSchema.addProp(CONNECT_INTERNAL_TYPE_NAME, MAP_ENTRY_TYPE_NAME)
                        }
                        addAvroRecordField(
                            fields,
                            KEY_FIELD,
                            schema.keySchema(),
                            null,
                            fromConnectContext,
                        )
                        addAvroRecordField(
                            fields,
                            VALUE_FIELD,
                            schema.valueSchema(),
                            null,
                            fromConnectContext,
                        )
                        mapSchema.fields = fields
                        ApacheAvroSchema.createArray(mapSchema)
                    }

                Schema.Type.STRUCT ->
                    if (AVRO_TYPE_UNION == schema.name()) {
                        val unionSchemas: MutableList<ApacheAvroSchema> = ArrayList()
                        if (schema.isOptional) {
                            unionSchemas.add(ApacheAvroSchemaBuilder.builder().nullType())
                        }
                        for (field in schema.fields()) {
                            unionSchemas.add(
                                fromConnectSchemaWithCycle(nonOptional(field.schema()), fromConnectContext, true),
                            )
                        }
                        ApacheAvroSchema.createUnion(unionSchemas)
                    } else if (schema.isOptional) {
                        val unionSchemas: MutableList<ApacheAvroSchema> = ArrayList()
                        unionSchemas.add(ApacheAvroSchemaBuilder.builder().nullType())
                        unionSchemas.add(
                            fromConnectSchemaWithCycle(nonOptional(schema), fromConnectContext, false),
                        )
                        ApacheAvroSchema.createUnion(unionSchemas)
                    } else {
                        val doc =
                            if (schema.parameters() != null) {
                                schema.parameters()[AVRO_RECORD_DOC_PROP]
                            } else {
                                null
                            }
                        val recordSchema =
                            ApacheAvroSchema.createRecord(
                                name ?: DEFAULT_SCHEMA_NAME,
                                doc,
                                namespace,
                                false,
                            )
                        if (schema.name() != null) {
                            fromConnectContext.cycleReferences[schema.name()] = recordSchema
                        }
                        val fields: MutableList<ApacheAvroSchema.Field> = ArrayList()
                        for (field in schema.fields()) {
                            val fieldDoc =
                                if (schema.parameters() != null) {
                                    schema.parameters()[AVRO_FIELD_DOC_PREFIX_PROP + field.name()]
                                } else {
                                    null
                                }
                            addAvroRecordField(fields, field.name(), field.schema(), fieldDoc, fromConnectContext)
                        }
                        recordSchema.fields = fields
                        recordSchema
                    }

                else -> throw DataException("Unknown schema type: " + schema.type())
            }

        var finalSchema = baseSchema
        if (baseSchema.type != ApacheAvroSchema.Type.UNION) {
            if (connectMetaData) {
                if (schema.doc() != null) {
                    baseSchema.addProp(CONNECT_DOC_PROP, schema.doc())
                }
                if (schema.version() != null) {
                    baseSchema.addProp(
                        CONNECT_VERSION_PROP,
                        JsonNodeFactory.instance.numberNode(schema.version()),
                    )
                }
                if (schema.parameters() != null) {
                    val params = parametersFromConnect(schema.parameters())
                    if (!params.isEmpty) {
                        baseSchema.addProp(CONNECT_PARAMETERS_PROP, params)
                    }
                }
                if (schema.defaultValue() != null) {
                    if (schema.parameters() == null ||
                        !schema.parameters().containsKey(AVRO_FIELD_DEFAULT_FLAG_PROP)
                    ) {
                        baseSchema.addProp(
                            CONNECT_DEFAULT_VALUE_PROP,
                            defaultValueFromConnect(schema, schema.defaultValue()),
                        )
                    }
                }
                if (schema.name() != null) {
                    baseSchema.addProp(CONNECT_NAME_PROP, schema.name())
                }
                // Some Connect types need special annotations to preserve the types accurate due to
                // limitations in Avro. These types get an extra annotation with their Connect type
                if (connectType != null) {
                    baseSchema.addProp(CONNECT_TYPE_PROP, connectType)
                }
            }

            var forceLegacyDecimal = false
            // the new and correct way to handle logical types
            if (schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    val precisionString = schema.parameters()[CONNECT_AVRO_DECIMAL_PRECISION_PROP]
                    val scaleString = schema.parameters()[Decimal.SCALE_FIELD]
                    val precision =
                        if (precisionString == null) CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT else precisionString.toInt()
                    val scale = if (scaleString == null) 0 else scaleString.toInt()
                    if (scale < 0 || scale > precision) {
                        log.trace(
                            "Scale and precision of {} and {} cannot be serialized as native Avro logical " +
                                "decimal type; reverting to legacy serialization method",
                            scale,
                            precision,
                        )
                        // We cannot use the Avro Java library's support for the decimal logical type when the
                        // scale is either negative or greater than the precision as this violates the Avro spec
                        // and causes the Avro library to throw an exception, so we fall back in this case to
                        // using the legacy method for encoding decimal logical type information.
                        // Can't add a key/value pair with the CONNECT_AVRO_DECIMAL_PRECISION_PROP key to the
                        // schema's parameters since the parameters for Connect schemas are immutable, so we
                        // just track this in a local boolean variable instead.
                        forceLegacyDecimal = true
                    } else {
                        LogicalTypes.decimal(precision, scale).addToSchema(baseSchema)
                    }
                } else if (Time.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    LogicalTypes.timeMillis().addToSchema(baseSchema)
                } else if (Timestamp.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    LogicalTypes.timestampMillis().addToSchema(baseSchema)
                } else if (Date.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    LogicalTypes.date().addToSchema(baseSchema)
                }
            }

            // Initially, to add support for logical types a new property was added
            // with key `logicalType`. This enabled logical types for avro schemas but not others,
            // such as parquet. The use of 'addToSchema` above supersedes this method here,
            //  which should eventually be removed.
            // Keeping for backwards compatibility until a major version upgrade happens.

            // Below follows the older method of supporting logical types via properties.
            // It is retained for now and will be deprecated eventually.
            // Only Avro named types (record, enum, fixed) may contain namespace + name. Only Connect's
            // struct converts to one of those (record), so for everything else that has a name we store
            // the full name into a special property. For uniformity, we also duplicate this info into
            // the same field in records as well even though it will also be available in the namespace()
            // and name().
            if (schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equals(schema.name(), ignoreCase = true) &&
                    (
                        schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP) ||
                            forceLegacyDecimal
                        )
                ) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DECIMAL)
                } else if (Time.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIME_MILLIS)
                } else if (Timestamp.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIMESTAMP_MILLIS)
                } else if (Date.LOGICAL_NAME.equals(schema.name(), ignoreCase = true)) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DATE)
                }
            }

            if (schema.parameters() != null) {
                for (entry in schema.parameters().entries) {
                    if (entry.key.startsWith(AVRO_PROP)) {
                        baseSchema.addProp(entry.key, entry.value)
                    }
                }
            }

            // Note that all metadata has already been processed and placed on the baseSchema because we
            // can't store any metadata on the actual top-level schema when it's a union because of Avro
            // constraints on the format of schemas.
            if (!ignoreOptional) {
                if (schema.isOptional) {
                    finalSchema =
                        if (schema.defaultValue() != null) {
                            ApacheAvroSchemaBuilder
                                .builder()
                                .unionOf()
                                .type(baseSchema)
                                .and()
                                .nullType()
                                .endUnion()
                        } else {
                            ApacheAvroSchemaBuilder
                                .builder()
                                .unionOf()
                                .nullType()
                                .and()
                                .type(baseSchema)
                                .endUnion()
                        }
                }
            }
        }

        if (!schema.isOptional) {
            fromConnectContext.schemaMap[schema] = finalSchema
        }
        fromConnectSchemaCache.put(schema, finalSchema)
        return finalSchema
    }

    open fun fromConnectSchemaWithCycle(
        schema: Schema,
        fromConnectContext: FromConnectContext,
        ignoreOptional: Boolean,
    ): ApacheAvroSchema = fromConnectContext.cycleReferences[schema.name()]
        ?: fromConnectSchema(schema, fromConnectContext, ignoreOptional)

    private fun addAvroRecordField(
        fields: MutableList<ApacheAvroSchema.Field>,
        fieldName: String,
        fieldSchema: Schema,
        fieldDoc: String?,
        fromConnectContext: FromConnectContext,
    ) {
        var defaultVal: Any? = null
        if (fieldSchema.defaultValue() != null) {
            defaultVal =
                JacksonUtils.toObject(
                    defaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()),
                )
        } else if (fieldSchema.isOptional) {
            defaultVal = JsonProperties.NULL_VALUE
        }
        val field =
            ApacheAvroSchema.Field(
                fieldName,
                fromConnectSchema(fieldSchema, fromConnectContext, false),
                fieldDoc,
                defaultVal,
            )
        fields.add(field)
    }

    private fun isMapEntry(elemSchema: ApacheAvroSchema): Boolean {
        if (elemSchema.type != ApacheAvroSchema.Type.RECORD) {
            return false
        }
        if (NAMESPACE == elemSchema.namespace && MAP_ENTRY_TYPE_NAME == elemSchema.name) {
            return true
        }
        if (Objects.equals(elemSchema.getProp(CONNECT_INTERNAL_TYPE_NAME), MAP_ENTRY_TYPE_NAME)) {
            return true
        }
        return false
    }

    /**
     * Convert the given object, in Avro format, into a Connect data object.
     * @param avroSchema the Avro schema
     * @param value the value to convert into a Connect data object
     * @return the Connect schema and value
     */
    open fun toConnectData(
        avroSchema: ApacheAvroSchema?,
        value: Any?,
    ): SchemaAndValue? = toConnectData(avroSchema, value, null)

    /**
     * Convert the given object, in Avro format, into a Connect data object.
     * @param avroSchema the Avro schema
     * @param value the value to convert into a Connect data object
     * @param version the version to set on the Connect schema if the avroSchema does not have a
     *     property named "connect.version", may be null
     * @return the Connect schema and value
     */
    open fun toConnectData(
        avroSchema: ApacheAvroSchema?,
        value: Any?,
        version: Int?,
    ): SchemaAndValue? {
        if (value == null) {
            return null
        }
        val toConnectContext = ToConnectContext()
        val schema =
            if (avroSchema == ANYTHING_SCHEMA) {
                null
            } else {
                toConnectSchema(avroSchema!!, version, toConnectContext)
            }
        return SchemaAndValue(schema, toConnectData(schema, value, toConnectContext))
    }

    private fun toConnectData(
        schema: Schema?,
        value: Any?,
        toConnectContext: ToConnectContext,
    ): Any? = toConnectData(schema, value, toConnectContext, true)

    private fun toConnectData(
        schema: Schema?,
        value: Any?,
        toConnectContext: ToConnectContext,
        doLogicalConversion: Boolean,
    ): Any? {
        validateSchemaValue(schema, value)
        if (value == null || value === JsonProperties.NULL_VALUE) {
            return null
        }
        try {
            // If we're decoding schemaless data, we need to unwrap it into just the single value
            if (schema == null) {
                if (value !is IndexedRecord) {
                    throw DataException("Invalid Avro data for schemaless Connect data")
                }
                val recordValue: IndexedRecord = value

                val boolVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BOOLEAN_FIELD).pos())
                if (boolVal != null) {
                    return toConnectData(Schema.BOOLEAN_SCHEMA, boolVal, toConnectContext)
                }

                val bytesVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BYTES_FIELD).pos())
                if (bytesVal != null) {
                    return toConnectData(Schema.BYTES_SCHEMA, bytesVal, toConnectContext)
                }

                val dblVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_DOUBLE_FIELD).pos())
                if (dblVal != null) {
                    return toConnectData(Schema.FLOAT64_SCHEMA, dblVal, toConnectContext)
                }

                val fltVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_FLOAT_FIELD).pos())
                if (fltVal != null) {
                    return toConnectData(Schema.FLOAT32_SCHEMA, fltVal, toConnectContext)
                }

                val intVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_INT_FIELD).pos())
                if (intVal != null) {
                    return toConnectData(Schema.INT32_SCHEMA, intVal, toConnectContext)
                }

                val longVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_LONG_FIELD).pos())
                if (longVal != null) {
                    return toConnectData(Schema.INT64_SCHEMA, longVal, toConnectContext)
                }

                val stringVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_STRING_FIELD).pos())
                if (stringVal != null) {
                    return toConnectData(Schema.STRING_SCHEMA, stringVal, toConnectContext)
                }

                val arrayVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_ARRAY_FIELD).pos())
                if (arrayVal != null) {
                    // We cannot reuse the logic like we do in other cases because it is not possible to
                    // construct an array schema with a null item schema, but the items have no schema.
                    if (arrayVal !is Collection<*>) {
                        throw DataException(
                            "Expected a Collection for schemaless array field but found a " +
                                arrayVal.javaClass.name,
                        )
                    }
                    val original = arrayVal as Collection<Any?>
                    val result: MutableList<Any?> = ArrayList(original.size)
                    for (elem in original) {
                        result.add(toConnectData(null as Schema?, elem, toConnectContext))
                    }
                    return result
                }

                val mapVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_MAP_FIELD).pos())
                if (mapVal != null) {
                    // We cannot reuse the logic like we do in other cases because it is not possible to
                    // construct a map schema with a null item schema, but the items have no schema.
                    if (mapVal !is Collection<*>) {
                        throw DataException(
                            "Expected a List for schemaless map field but found a " +
                                mapVal.javaClass.name,
                        )
                    }
                    val original = mapVal as Collection<IndexedRecord>
                    val result: MutableMap<Any?, Any?> = HashMap(original.size)
                    for (entry in original) {
                        val avroKeyFieldIndex = entry.schema.getField(KEY_FIELD).pos()
                        val avroValueFieldIndex = entry.schema.getField(VALUE_FIELD).pos()
                        val convertedKey = toConnectData(null, entry.get(avroKeyFieldIndex), toConnectContext)
                        val convertedValue = toConnectData(null, entry.get(avroValueFieldIndex), toConnectContext)
                        result[convertedKey] = convertedValue
                    }
                    return result
                }

                // If nothing was set, it's null
                return null
            }

            var converted: Any? = null
            when (schema.type()) {
                // Pass through types
                Schema.Type.INT32 -> {
                    val intValue = value as Int // Validate type
                    converted = value
                }

                Schema.Type.INT64 -> {
                    val longValue = value as Long // Validate type
                    converted = value
                }

                Schema.Type.FLOAT32 -> {
                    val floatValue = value as Float // Validate type
                    converted = value
                }

                Schema.Type.FLOAT64 -> {
                    val doubleValue = value as Double // Validate type
                    converted = value
                }

                Schema.Type.BOOLEAN -> {
                    val boolValue = value as Boolean // Validate type
                    converted = value
                }

                Schema.Type.INT8 ->
                    // Encoded as an Integer
                    converted = (value as Int).toByte()

                Schema.Type.INT16 ->
                    // Encoded as an Integer
                    converted = (value as Int).toShort()

                Schema.Type.STRING ->
                    converted =
                        if (value is String) {
                            value
                        } else if (value is CharSequence || value is GenericEnumSymbol<*> || value is Enum<*>) {
                            value.toString()
                        } else {
                            throw DataException(
                                "Invalid class for string type, expecting String or " +
                                    "CharSequence but found " + value.javaClass,
                            )
                        }

                Schema.Type.BYTES ->
                    converted =
                        if (value is ByteArray) {
                            ByteBuffer.wrap(value)
                        } else if (value is ByteBuffer) {
                            value
                        } else if (value is GenericFixed) {
                            ByteBuffer.wrap(value.bytes())
                        } else if (value is BigDecimal && Decimal.LOGICAL_NAME == schema.name()) {
                            ByteBuffer.wrap(Decimal.fromLogical(schema, value))
                        } else {
                            throw DataException(
                                "Invalid class for bytes type, expecting byte[], ByteBuffer, GenericFixed " +
                                    "or a BigDecimal on a decimal schema but found " + value.javaClass,
                            )
                        }

                Schema.Type.ARRAY -> {
                    val valueSchema = schema.valueSchema()
                    val original = value as Collection<Any?>
                    val result: MutableList<Any?> = ArrayList(original.size)
                    for (elem in original) {
                        result.add(toConnectData(valueSchema, elem, toConnectContext))
                    }
                    converted = result
                }

                Schema.Type.MAP -> {
                    val keySchema = schema.keySchema()
                    val valueSchema = schema.valueSchema()
                    converted =
                        if (keySchema != null && keySchema.type() == Schema.Type.STRING && !keySchema.isOptional) {
                            // Non-optional string keys
                            val original = value as Map<CharSequence, Any?>
                            val result: MutableMap<CharSequence, Any?> = HashMap(original.size)
                            for (entry in original.entries) {
                                // Bound to a typed local so the checkcast is actually emitted
                                val entryKey: CharSequence = entry.key
                                result[entryKey.toString()] =
                                    toConnectData(valueSchema, entry.value, toConnectContext)
                            }
                            result
                        } else {
                            // Arbitrary keys
                            val original = value as Collection<IndexedRecord>
                            val result: MutableMap<Any?, Any?> = HashMap(original.size)
                            for (entry in original) {
                                val avroKeyFieldIndex = entry.schema.getField(KEY_FIELD).pos()
                                val avroValueFieldIndex = entry.schema.getField(VALUE_FIELD).pos()
                                val convertedKey =
                                    toConnectData(keySchema, entry.get(avroKeyFieldIndex), toConnectContext)
                                val convertedValue =
                                    toConnectData(valueSchema, entry.get(avroValueFieldIndex), toConnectContext)
                                result[convertedKey] = convertedValue
                            }
                            result
                        }
                }

                Schema.Type.STRUCT -> {
                    // Special case support for union types
                    if (schema.name() != null && schema.name() == AVRO_TYPE_UNION) {
                        var valueRecordSchema: Schema? = null
                        if (value is IndexedRecord) {
                            valueRecordSchema =
                                toConnectSchemaWithCycles(value.schema, true, null, null, toConnectContext)
                        }
                        for (field in schema.fields()) {
                            val fieldSchema = field.schema()

                            if (isInstanceOfAvroSchemaTypeForSimpleSchema(fieldSchema, value) ||
                                (valueRecordSchema != null && schemaEquals(valueRecordSchema, fieldSchema))
                            ) {
                                converted =
                                    Struct(schema).put(
                                        unionMemberFieldName(fieldSchema),
                                        toConnectData(fieldSchema, value, toConnectContext),
                                    )
                                break
                            }
                        }
                        if (converted == null) {
                            throw DataException("Did not find matching union field for data: $value")
                        }
                    } else if (value is Map<*, *>) {
                        // Default values from Avro are returned as Map
                        val original = value as Map<CharSequence, Any?>
                        val result = Struct(schema)
                        for (field in schema.fields()) {
                            val convertedFieldValue =
                                toConnectData(field.schema(), original[field.name()], toConnectContext)
                            result.put(field, convertedFieldValue)
                        }
                        return result
                    } else {
                        val original = value as IndexedRecord
                        val result = Struct(schema)
                        for (field in schema.fields()) {
                            val avroFieldIndex = original.schema.getField(field.name()).pos()
                            val convertedFieldValue =
                                toConnectData(field.schema(), original.get(avroFieldIndex), toConnectContext)
                            result.put(field, convertedFieldValue)
                        }
                        converted = result
                    }
                }

                else -> throw DataException("Unknown Connect schema type: " + schema.type())
            }

            if (schema.name() != null && doLogicalConversion) {
                val logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS[schema.name()]
                if (logicalConverter != null) {
                    converted = logicalConverter.convert(schema, converted)
                }
            }
            return converted
        } catch (e: ClassCastException) {
            val schemaType = if (schema != null) schema.type().toString() else "null"
            throw DataException("Invalid type for " + schemaType + ": " + value.javaClass)
        }
    }

    protected open fun getForceOptionalDefault(): Boolean = false

    open fun toConnectSchema(schema: ApacheAvroSchema): Schema = toConnectSchema(schema, null, ToConnectContext())

    private fun toConnectSchema(
        schema: ApacheAvroSchema,
        version: Int?,
        toConnectContext: ToConnectContext,
    ): Schema {
        // We perform caching only at this top level. While it might be helpful to cache some more of
        // the internal conversions, this is the safest place to add caching since some of the internal
        // conversions take extra flags (like forceOptional) which means the resulting schema might not
        // exactly match the Avro schema.
        val schemaAndVersion = AvroSchema(schema, version)
        val cachedSchema = toConnectSchemaCache.get(schemaAndVersion)
        if (cachedSchema != null) {
            if (schema.type == ApacheAvroSchema.Type.RECORD) {
                // cycleReferences is only populated with record type schemas. We need to initialize it here
                // with the top-level record schema, as would happen if we did not hit the cache. This
                // schema has the version information set, thus it properly works with schemaEquals.
                toConnectContext.cycleReferences[schema] = CyclicSchemaWrapper(cachedSchema)
            }
            return cachedSchema
        }

        val resultSchema =
            toConnectSchema(schema, getForceOptionalDefault(), null, null, version, toConnectContext)
        toConnectSchemaCache.put(schemaAndVersion, resultSchema)
        return resultSchema
    }

    private fun toConnectSchema(
        schema: ApacheAvroSchema,
        forceOptional: Boolean,
        fieldDefaultVal: Any?,
        docDefaultVal: String?,
        toConnectContext: ToConnectContext,
    ): Schema = toConnectSchema(schema, forceOptional, fieldDefaultVal, docDefaultVal, null, toConnectContext)

    /**
     * @param schema           schema to convert
     * @param forceOptional    make the resulting schema optional, for converting Avro unions to a
     *                         record format and simple Avro unions of null + type to optional schemas
     * @param fieldDefaultVal  if non-null, override any connect-annotated default values with this
     *                         one; used when converting Avro record fields since they define default
     *                         values with the field spec, but Connect specifies them with the field's
     *                         schema
     * @param docDefaultVal    if non-null, override any connect-annotated documentation with this
     *                         one;
     *                         used when converting Avro record fields since they define doc values
     * @param toConnectContext context object that holds state while doing the conversion
     */
    private fun toConnectSchema(
        schema: ApacheAvroSchema,
        forceOptional: Boolean,
        fieldDefaultVal: Any?,
        docDefaultVal: String?,
        version: Int?,
        toConnectContext: ToConnectContext,
    ): Schema {
        var fieldDefault = fieldDefaultVal
        val type = schema.getProp(CONNECT_TYPE_PROP)
        val logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP)

        val builder: SchemaBuilder =
            when (schema.type) {
                ApacheAvroSchema.Type.BOOLEAN -> SchemaBuilder.bool()

                ApacheAvroSchema.Type.BYTES, ApacheAvroSchema.Type.FIXED ->
                    if (AVRO_LOGICAL_DECIMAL.equals(logicalType, ignoreCase = true)) {
                        val scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP)
                        if (scaleNode !is Number) {
                            throw DataException("scale must be specified and must be a number.")
                        }
                        val decimalBuilder = Decimal.builder(scaleNode.toInt())

                        val precisionNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP)
                        if (null != precisionNode) {
                            if (precisionNode !is Number) {
                                throw DataException(
                                    AVRO_LOGICAL_DECIMAL_PRECISION_PROP +
                                        " property must be a JSON Integer." +
                                        " https://avro.apache.org/docs/1.9.1/spec.html#Decimal",
                                )
                            }
                            // Capture the precision as a parameter only if it is not the default
                            val precision = precisionNode.toInt()
                            if (precision != CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT) {
                                decimalBuilder.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, precision.toString())
                            }
                        }
                        decimalBuilder
                    } else {
                        SchemaBuilder.bytes()
                    }

                ApacheAvroSchema.Type.DOUBLE -> SchemaBuilder.float64()
                ApacheAvroSchema.Type.FLOAT -> SchemaBuilder.float32()

                ApacheAvroSchema.Type.INT ->
                    // INT is used for Connect's INT8, INT16, and INT32
                    if (type == null && logicalType == null) {
                        SchemaBuilder.int32()
                    } else if (logicalType != null) {
                        if (AVRO_LOGICAL_DATE.equals(logicalType, ignoreCase = true)) {
                            Date.builder()
                        } else if (AVRO_LOGICAL_TIME_MILLIS.equals(logicalType, ignoreCase = true)) {
                            Time.builder()
                        } else {
                            SchemaBuilder.int32()
                        }
                    } else {
                        val connectType =
                            NON_AVRO_TYPES_BY_TYPE_CODE[type]
                                ?: throw DataException("Connect type annotation for Avro int field is null")
                        SchemaBuilder.type(connectType)
                    }

                ApacheAvroSchema.Type.LONG ->
                    if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equals(logicalType, ignoreCase = true)) {
                        Timestamp.builder()
                    } else {
                        SchemaBuilder.int64()
                    }

                ApacheAvroSchema.Type.STRING -> SchemaBuilder.string()

                ApacheAvroSchema.Type.ARRAY -> {
                    val elemSchema = schema.elementType
                    // Special case for custom encoding of non-string maps as list of key-value records
                    if (isMapEntry(elemSchema)) {
                        if (elemSchema.fields.size != 2 ||
                            elemSchema.getField(KEY_FIELD) == null ||
                            elemSchema.getField(VALUE_FIELD) == null
                        ) {
                            throw DataException(
                                "Found map encoded as array of key-value pairs, but array " +
                                    "elements do not match the expected format.",
                            )
                        }
                        SchemaBuilder.map(
                            toConnectSchema(elemSchema.getField(KEY_FIELD).schema()),
                            toConnectSchema(elemSchema.getField(VALUE_FIELD).schema()),
                        )
                    } else {
                        val arraySchema =
                            toConnectSchemaWithCycles(
                                schema.elementType,
                                getForceOptionalDefault(),
                                null,
                                null,
                                toConnectContext,
                            )
                        SchemaBuilder.array(arraySchema)
                    }
                }

                ApacheAvroSchema.Type.MAP ->
                    SchemaBuilder.map(
                        Schema.STRING_SCHEMA,
                        toConnectSchemaWithCycles(
                            schema.valueType,
                            getForceOptionalDefault(),
                            null,
                            null,
                            toConnectContext,
                        ),
                    )

                ApacheAvroSchema.Type.RECORD -> {
                    val recordBuilder = SchemaBuilder.struct()
                    toConnectContext.cycleReferences[schema] = CyclicSchemaWrapper(recordBuilder)
                    if (connectMetaData && schema.doc != null) {
                        recordBuilder.parameter(AVRO_RECORD_DOC_PROP, schema.doc)
                    }
                    for (field in schema.fields) {
                        if (connectMetaData && field.doc() != null) {
                            recordBuilder.parameter(AVRO_FIELD_DOC_PREFIX_PROP + field.name(), field.doc())
                        }
                        val fieldSchema =
                            toConnectSchema(
                                field.schema(),
                                getForceOptionalDefault(),
                                field.defaultVal(),
                                field.doc(),
                                toConnectContext,
                            )
                        recordBuilder.field(field.name(), fieldSchema)
                    }
                    recordBuilder
                }

                ApacheAvroSchema.Type.ENUM -> {
                    // enums are unwrapped to strings and the original enum is not preserved
                    val enumBuilder = SchemaBuilder.string()
                    if (connectMetaData) {
                        if (schema.doc != null) {
                            enumBuilder.parameter(AVRO_ENUM_DOC_PREFIX_PROP + schema.name, schema.doc)
                        }
                        if (schema.enumDefault != null) {
                            enumBuilder.parameter(
                                AVRO_ENUM_DEFAULT_PREFIX_PROP + schema.name,
                                schema.enumDefault,
                            )
                        }
                    }
                    enumBuilder.parameter(AVRO_TYPE_ENUM, schema.fullName)
                    for (enumSymbol in schema.enumSymbols) {
                        enumBuilder.parameter("$AVRO_TYPE_ENUM.$enumSymbol", enumSymbol)
                    }
                    enumBuilder
                }

                ApacheAvroSchema.Type.UNION -> {
                    if (schema.types.size == 2) {
                        if (schema.types.contains(NULL_AVRO_SCHEMA)) {
                            for (memberSchema in schema.types) {
                                if (memberSchema != NULL_AVRO_SCHEMA) {
                                    return toConnectSchemaWithCycles(
                                        memberSchema,
                                        true,
                                        null,
                                        docDefaultVal,
                                        toConnectContext,
                                    )
                                }
                            }
                        }
                    }
                    val unionBuilder = SchemaBuilder.struct().name(AVRO_TYPE_UNION)
                    val fieldNames: MutableSet<String> = HashSet()
                    for (memberSchema in schema.types) {
                        if (memberSchema.type == ApacheAvroSchema.Type.NULL) {
                            unionBuilder.optional()
                        } else {
                            val fieldName = unionMemberFieldName(memberSchema)
                            if (fieldNames.contains(fieldName)) {
                                throw DataException("Multiple union schemas map to the Connect union field name")
                            }
                            fieldNames.add(fieldName)
                            unionBuilder.field(
                                fieldName,
                                toConnectSchemaWithCycles(memberSchema, true, null, null, toConnectContext),
                            )
                        }
                    }
                    unionBuilder
                }

                ApacheAvroSchema.Type.NULL ->
                    // There's no dedicated null type in Connect. However, it also doesn't make sense to have a
                    // standalone null type -- it wouldn't provide any useful information. Instead, it should
                    // only be used in union types.
                    throw DataException("Standalone null schemas are not supported by this converter")

                else ->
                    throw DataException(
                        "Couldn't translate unsupported schema type " + schema.type.getName() + ".",
                    )
            }

        val docVal = schema.getProp(CONNECT_DOC_PROP)
        if (connectMetaData && docVal != null) {
            builder.doc(docVal)
        }

        // Included Kafka Connect version takes priority, fall back to schema registry version
        var versionInt = -1 // A valid version must be a positive integer (assumed throughout SR)
        val versionNode = schema.getObjectProp(CONNECT_VERSION_PROP)
        if (versionNode != null) {
            if (versionNode !is Number) {
                throw DataException("Invalid Connect version found: $versionNode")
            }
            versionInt = versionNode.toInt()
        } else if (version != null) {
            versionInt = version.toInt()
        }
        if (versionInt >= 0) {
            if (builder.version() != null) {
                if (versionInt != builder.version()) {
                    throw DataException(
                        "Mismatched versions: version already added to SchemaBuilder " +
                            "(" +
                            builder.version() +
                            ") differs from version in source schema (" +
                            versionInt +
                            ")",
                    )
                }
            } else {
                builder.version(versionInt)
            }
        }

        val parameters = schema.getObjectProp(CONNECT_PARAMETERS_PROP)
        if (connectMetaData && parameters != null) {
            if (parameters !is Map<*, *>) {
                throw DataException("Expected JSON object for schema parameters but found: $parameters")
            }
            val paramIt = (parameters as Map<String, Any?>).entries.iterator()
            while (paramIt.hasNext()) {
                val field = paramIt.next()
                val jsonValue = field.value
                if (jsonValue !is String) {
                    throw DataException("Expected schema parameter values to be strings but found: $jsonValue")
                }
                builder.parameter(field.key, jsonValue)
            }
        }

        for (entry in schema.objectProps.entries) {
            if (entry.key.startsWith(AVRO_PROP)) {
                builder.parameter(entry.key, entry.value.toString())
            }
        }

        val connectDefault = schema.getObjectProp(CONNECT_DEFAULT_VALUE_PROP)
        if (fieldDefault == null) {
            fieldDefault = JacksonUtils.toJsonNode(connectDefault)
        } else if (connectDefault == null) {
            builder.parameter(AVRO_FIELD_DEFAULT_FLAG_PROP, "true")
        }
        if (fieldDefault != null) {
            builder.defaultValue(defaultValueFromAvro(builder, schema, fieldDefault, toConnectContext))
        }

        val connectNameJson = schema.getObjectProp(CONNECT_NAME_PROP)
        var name: String? = null
        if (connectNameJson != null) {
            if (connectNameJson !is String) {
                throw DataException("Invalid schema name: $connectNameJson")
            }
            name = connectNameJson
        } else if (schema.type == ApacheAvroSchema.Type.RECORD || schema.type == ApacheAvroSchema.Type.ENUM) {
            name = schema.fullName
        }
        if (name != null && name != DEFAULT_SCHEMA_FULL_NAME) {
            if (builder.name() != null) {
                if (name != builder.name()) {
                    throw DataException(
                        "Mismatched names: name already added to SchemaBuilder (" +
                            builder.name() +
                            ") differs from name in source schema (" +
                            name + ")",
                    )
                }
            } else {
                builder.name(name)
            }
        }

        if (forceOptional) {
            builder.optional()
        }

        if (!toConnectContext.detectedCycles.contains(schema) &&
            toConnectContext.cycleReferences.containsKey(schema)
        ) {
            toConnectContext.cycleReferences.remove(schema)
        }

        return builder.build()
    }

    private fun toConnectSchemaWithCycles(
        schema: ApacheAvroSchema,
        forceOptional: Boolean,
        fieldDefaultVal: Any?,
        docDefaultVal: String?,
        toConnectContext: ToConnectContext,
    ): Schema = if (toConnectContext.cycleReferences.containsKey(schema)) {
        toConnectContext.detectedCycles.add(schema)
        cyclicSchemaWrapper(toConnectContext.cycleReferences, schema, forceOptional)
    } else {
        toConnectSchema(schema, forceOptional, fieldDefaultVal, docDefaultVal, toConnectContext)
    }

    private fun cyclicSchemaWrapper(
        toConnectCycles: Map<ApacheAvroSchema, CyclicSchemaWrapper>,
        memberSchema: ApacheAvroSchema,
        optional: Boolean,
    ): CyclicSchemaWrapper = CyclicSchemaWrapper(toConnectCycles[memberSchema]!!.schema(), optional)

    private fun defaultValueFromAvro(
        schema: Schema,
        avroSchema: ApacheAvroSchema,
        value: Any?,
        toConnectContext: ToConnectContext,
    ): Any? {
        val result = defaultValueFromAvroWithoutLogical(schema, avroSchema, value, toConnectContext)
        // If the schema is a logical type, convert the primitive Avro default into the logical form
        return toConnectLogical(schema, result)
    }

    private fun defaultValueFromAvroWithoutLogical(
        schema: Schema,
        avroSchema: ApacheAvroSchema,
        value: Any?,
        toConnectContext: ToConnectContext,
    ): Any? {
        if (value == null || value === JsonProperties.NULL_VALUE) {
            return null
        }

        // The type will be JsonNode if this default was pulled from a Connect default field, or an
        // Object if it's the actual Avro-specified default. If it's a regular Java object, we can
        // use our existing conversion tools.
        if (value !is JsonNode) {
            return toConnectData(schema, value, toConnectContext, false)
        }

        val jsonValue: JsonNode = value
        when (avroSchema.type) {
            ApacheAvroSchema.Type.INT ->
                if (schema.type() == Schema.Type.INT8) {
                    return jsonValue.intValue().toByte()
                } else if (schema.type() == Schema.Type.INT16) {
                    return jsonValue.shortValue()
                } else if (schema.type() == Schema.Type.INT32) {
                    return jsonValue.intValue()
                }

            ApacheAvroSchema.Type.LONG -> return jsonValue.longValue()

            ApacheAvroSchema.Type.FLOAT -> return jsonValue.doubleValue().toFloat()
            ApacheAvroSchema.Type.DOUBLE -> return jsonValue.doubleValue()

            ApacheAvroSchema.Type.BOOLEAN -> return jsonValue.asBoolean()

            ApacheAvroSchema.Type.NULL -> return null

            ApacheAvroSchema.Type.STRING, ApacheAvroSchema.Type.ENUM -> return jsonValue.asText()

            ApacheAvroSchema.Type.BYTES, ApacheAvroSchema.Type.FIXED ->
                try {
                    val bytes: ByteArray? =
                        if (jsonValue.isTextual) {
                            // Avro's JSON form may be a quoted string, so decode the binary value
                            val encoded = jsonValue.textValue()
                            encoded.toByteArray(StandardCharsets.ISO_8859_1)
                        } else {
                            jsonValue.binaryValue()
                        }
                    return if (bytes == null) null else ByteBuffer.wrap(bytes)
                } catch (e: IOException) {
                    throw DataException("Invalid binary data in default value", e)
                }

            ApacheAvroSchema.Type.ARRAY -> {
                if (!jsonValue.isArray) {
                    throw DataException("Invalid JSON for array default value: $jsonValue")
                }
                val result: MutableList<Any?> = ArrayList(jsonValue.size())
                for (elem in jsonValue) {
                    result.add(defaultValueFromAvro(schema, avroSchema.elementType, elem, toConnectContext))
                }
                return result
            }

            ApacheAvroSchema.Type.MAP -> {
                if (!jsonValue.isObject) {
                    throw DataException("Invalid JSON for map default value: $jsonValue")
                }
                val result: MutableMap<String, Any?> = HashMap(jsonValue.size())
                val fieldIt = jsonValue.fields()
                while (fieldIt.hasNext()) {
                    val field = fieldIt.next()
                    val converted =
                        defaultValueFromAvro(
                            schema.valueSchema(),
                            avroSchema.valueType,
                            field.value,
                            toConnectContext,
                        )
                    result[field.key] = converted
                }
                return result
            }

            ApacheAvroSchema.Type.RECORD -> {
                if (!jsonValue.isObject) {
                    throw DataException("Invalid JSON for record default value: $jsonValue")
                }

                val result = Struct(schema)
                for (avroField in avroSchema.fields) {
                    val field = schema.field(avroField.name())
                    val fieldJson = (value as JsonNode).get(field.name())
                    val converted =
                        defaultValueFromAvro(field.schema(), avroField.schema(), fieldJson, toConnectContext)
                    result.put(avroField.name(), converted)
                }
                return result
            }

            ApacheAvroSchema.Type.UNION -> {
                // Defaults must match first type
                val memberAvroSchema = avroSchema.types[0]
                return if (memberAvroSchema.type == ApacheAvroSchema.Type.NULL) {
                    null
                } else {
                    defaultValueFromAvro(
                        schema.field(unionMemberFieldName(memberAvroSchema)).schema(),
                        memberAvroSchema,
                        value,
                        toConnectContext,
                    )
                }
            }

            else -> return null
        }
        return null
    }

    private fun unionMemberFieldName(schema: ApacheAvroSchema): String {
        if (schema.type == ApacheAvroSchema.Type.RECORD || schema.type == ApacheAvroSchema.Type.ENUM) {
            return if (enhancedSchemaSupport) {
                schema.fullName
            } else {
                splitName(schema.name)[1]!!
            }
        }
        return schema.type.getName()
    }

    private fun unionMemberFieldName(schema: Schema): String {
        if (schema.type() == Schema.Type.STRUCT || isEnumSchema(schema)) {
            return if (enhancedSchemaSupport) {
                schema.name()
            } else {
                splitName(schema.name())[1]!!
            }
        }
        return CONNECT_TYPES_TO_AVRO_TYPES[schema.type()]!!.getName()
    }

    private fun interface LogicalTypeConverter {
        fun convert(
            schema: Schema,
            value: Any?,
        ): Any?
    }

    /**
     * Class that holds the context for performing `toConnectSchema`
     */
    private class ToConnectContext {
        /** map that holds connect Schema references to resolve cycles */
        val cycleReferences: MutableMap<ApacheAvroSchema, CyclicSchemaWrapper> = IdentityHashMap()

        /** avro schemas that have been detected to have cycles */
        val detectedCycles: MutableSet<ApacheAvroSchema> = HashSet()
    }

    /**
     * Class that holds the context for performing `fromConnectSchema`
     */
    class FromConnectContext internal constructor(
        // SchemaMap is used to resolve references that need to mapped as types
        val schemaMap: MutableMap<Schema, ApacheAvroSchema>,
    ) {
        // schema name to Schema reference to resolve cycles
        val cycleReferences: MutableMap<String, ApacheAvroSchema> = IdentityHashMap()
    }

    private class SchemaPair(
        val one: Schema?,
        val two: Schema?,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) {
                return true
            }
            if (other == null || javaClass != other.javaClass) {
                return false
            }
            val that = other as SchemaPair
            return Objects.equals(one, that.one) && Objects.equals(two, that.two)
        }

        override fun hashCode(): Int = Objects.hash(one, two)
    }

    private class CyclicSchemaWrapper(
        private val schema: Schema,
        private val optional: Boolean,
    ) : Schema {
        constructor(schema: Schema) : this(schema, schema.isOptional)

        override fun type(): Schema.Type = schema.type()

        override fun isOptional(): Boolean = optional

        override fun defaultValue(): Any? = schema.defaultValue()

        override fun name(): String? = schema.name()

        override fun version(): Int? = schema.version()

        override fun doc(): String? = schema.doc()

        override fun parameters(): Map<String, String>? = schema.parameters()

        override fun keySchema(): Schema? = schema.keySchema()

        override fun valueSchema(): Schema? = schema.valueSchema()

        override fun fields(): List<Field>? = schema.fields()

        override fun field(s: String): Field? = schema.field(s)

        override fun schema(): Schema = schema

        override fun equals(other: Any?): Boolean {
            if (this === other) {
                return true
            }

            if (other == null || javaClass != other.javaClass) {
                return false
            }

            val that = other as CyclicSchemaWrapper
            return Objects.equals(optional, that.optional) && Objects.equals(schema, that.schema)
        }

        override fun hashCode(): Int = Objects.hashCode(optional) + Objects.hashCode(schema)
    }

    companion object {
        private val log = LoggerFactory.getLogger(AvroData::class.java)

        const val NAMESPACE = "com.amazonaws.services.schemaregistry.kafkaconnect.avrodata"

        // Avro does not permit empty schema names, which might be the ideal default since we also are
        // not permitted to simply omit the name. Instead, make it very clear where the default is
        // coming from.
        const val DEFAULT_SCHEMA_NAME = "ConnectDefault"
        const val DEFAULT_SCHEMA_FULL_NAME = "$NAMESPACE.$DEFAULT_SCHEMA_NAME"
        const val MAP_ENTRY_TYPE_NAME = "MapEntry"
        const val KEY_FIELD = "key"
        const val VALUE_FIELD = "value"

        const val CONNECT_NAME_PROP = "connect.name"
        const val CONNECT_DOC_PROP = "connect.doc"
        const val CONNECT_VERSION_PROP = "connect.version"
        const val CONNECT_DEFAULT_VALUE_PROP = "connect.default"
        const val CONNECT_PARAMETERS_PROP = "connect.parameters"
        const val CONNECT_INTERNAL_TYPE_NAME = "connect.internal.type"
        const val AVRO_RECORD_DOC_PROP = "$NAMESPACE.record.doc"
        const val AVRO_ENUM_DOC_PREFIX_PROP = "$NAMESPACE.enum.doc."
        const val AVRO_FIELD_DOC_PREFIX_PROP = "$NAMESPACE.field.doc."

        // This property is used to determine whether a default value in the Connect schema originated
        // from an Avro field default
        const val AVRO_FIELD_DEFAULT_FLAG_PROP = "$NAMESPACE.field.default"
        const val AVRO_ENUM_DEFAULT_PREFIX_PROP = "$NAMESPACE.enum.default."

        const val CONNECT_TYPE_PROP = "connect.type"

        const val CONNECT_TYPE_INT8 = "int8"
        const val CONNECT_TYPE_INT16 = "int16"

        const val AVRO_TYPE_UNION = "$NAMESPACE.Union"
        const val AVRO_TYPE_ENUM = "$NAMESPACE.Enum"

        const val AVRO_TYPE_ANYTHING = "$NAMESPACE.Anything"

        private val NON_AVRO_TYPES_BY_TYPE_CODE: MutableMap<String, Schema.Type> = HashMap()

        // Avro Java object types used by Connect schema types
        private val SIMPLE_AVRO_SCHEMA_TYPES: MutableMap<Schema.Type, List<Class<*>>> = HashMap()

        private val CONNECT_TYPES_TO_AVRO_TYPES: MutableMap<Schema.Type, ApacheAvroSchema.Type> = HashMap()

        private const val ANYTHING_SCHEMA_BOOLEAN_FIELD = "boolean"
        private const val ANYTHING_SCHEMA_BYTES_FIELD = "bytes"
        private const val ANYTHING_SCHEMA_DOUBLE_FIELD = "double"
        private const val ANYTHING_SCHEMA_FLOAT_FIELD = "float"
        private const val ANYTHING_SCHEMA_INT_FIELD = "int"
        private const val ANYTHING_SCHEMA_LONG_FIELD = "long"
        private const val ANYTHING_SCHEMA_STRING_FIELD = "string"
        private const val ANYTHING_SCHEMA_ARRAY_FIELD = "array"
        private const val ANYTHING_SCHEMA_MAP_FIELD = "map"

        // Intuitively this should be a union schema. However, unions can't be named in Avro and this
        // is a self-referencing type, so we need to use a format in which we can name the entire schema
        @JvmField
        val ANYTHING_SCHEMA: ApacheAvroSchema =
            ApacheAvroSchemaBuilder
                .record(AVRO_TYPE_ANYTHING)
                .namespace(NAMESPACE)
                .fields()
                .optionalBoolean(ANYTHING_SCHEMA_BOOLEAN_FIELD)
                .optionalBytes(ANYTHING_SCHEMA_BYTES_FIELD)
                .optionalDouble(ANYTHING_SCHEMA_DOUBLE_FIELD)
                .optionalFloat(ANYTHING_SCHEMA_FLOAT_FIELD)
                .optionalInt(ANYTHING_SCHEMA_INT_FIELD)
                .optionalLong(ANYTHING_SCHEMA_LONG_FIELD)
                .optionalString(ANYTHING_SCHEMA_STRING_FIELD)
                .name(ANYTHING_SCHEMA_ARRAY_FIELD)
                .type()
                .optional()
                .array()
                .items()
                .type(AVRO_TYPE_ANYTHING)
                .name(ANYTHING_SCHEMA_MAP_FIELD)
                .type()
                .optional()
                .array()
                .items()
                .record(MAP_ENTRY_TYPE_NAME)
                .namespace(NAMESPACE)
                .fields()
                .name(KEY_FIELD)
                .type(AVRO_TYPE_ANYTHING)
                .noDefault()
                .name(VALUE_FIELD)
                .type(AVRO_TYPE_ANYTHING)
                .noDefault()
                .endRecord()
                .endRecord()

        // This is convenient to have extracted; we can't define it before ANYTHING_SCHEMA because it
        // uses ANYTHING_SCHEMA in its definition.
        @JvmField
        val ANYTHING_SCHEMA_MAP_ELEMENT: ApacheAvroSchema =
            ANYTHING_SCHEMA
                .getField("map")
                .schema()
                .types[1] // The "map" field is optional, get the schema from the union type
                .elementType

        private val NULL_AVRO_SCHEMA: ApacheAvroSchema = ApacheAvroSchema.create(ApacheAvroSchema.Type.NULL)

        // Convert values in Connect form into their logical types. These logical converters are
        // discovered by logical type names specified in the field
        private val TO_CONNECT_LOGICAL_CONVERTERS = HashMap<String, LogicalTypeConverter>()

        const val AVRO_PROP = "avro"
        const val AVRO_LOGICAL_TYPE_PROP = "logicalType"
        const val AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis"
        const val AVRO_LOGICAL_TIME_MILLIS = "time-millis"
        const val AVRO_LOGICAL_DATE = "date"
        const val AVRO_LOGICAL_DECIMAL = "decimal"
        const val AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale"
        const val AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision"
        const val CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision"
        const val CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64

        private val TO_AVRO_LOGICAL_CONVERTERS = HashMap<String, LogicalTypeConverter>()

        init {
            NON_AVRO_TYPES_BY_TYPE_CODE[CONNECT_TYPE_INT8] = Schema.Type.INT8
            NON_AVRO_TYPES_BY_TYPE_CODE[CONNECT_TYPE_INT16] = Schema.Type.INT16

            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.INT32] = listOf(Integer::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.INT64] = listOf(java.lang.Long::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.FLOAT32] = listOf(java.lang.Float::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.FLOAT64] = listOf(java.lang.Double::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.BOOLEAN] = listOf(java.lang.Boolean::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.STRING] = listOf(CharSequence::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.BYTES] =
                listOf(ByteBuffer::class.java, ByteArray::class.java, GenericFixed::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.ARRAY] = listOf(Collection::class.java)
            SIMPLE_AVRO_SCHEMA_TYPES[Schema.Type.MAP] = listOf(Map::class.java)

            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.INT32] = ApacheAvroSchema.Type.INT
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.INT64] = ApacheAvroSchema.Type.LONG
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.FLOAT32] = ApacheAvroSchema.Type.FLOAT
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.FLOAT64] = ApacheAvroSchema.Type.DOUBLE
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.BOOLEAN] = ApacheAvroSchema.Type.BOOLEAN
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.STRING] = ApacheAvroSchema.Type.STRING
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.BYTES] = ApacheAvroSchema.Type.BYTES
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.ARRAY] = ApacheAvroSchema.Type.ARRAY
            CONNECT_TYPES_TO_AVRO_TYPES[Schema.Type.MAP] = ApacheAvroSchema.Type.MAP

            TO_CONNECT_LOGICAL_CONVERTERS[Decimal.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    when (value) {
                        is ByteArray -> Decimal.toLogical(schema, value)
                        is ByteBuffer -> Decimal.toLogical(schema, value.array())
                        else ->
                            throw DataException(
                                "Invalid type for Decimal, underlying representation should be bytes but was " +
                                    value!!.javaClass,
                            )
                    }
                }

            TO_CONNECT_LOGICAL_CONVERTERS[Date.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is Int) {
                        throw DataException(
                            "Invalid type for Date, underlying representation should be int32 but was " +
                                value!!.javaClass,
                        )
                    }
                    Date.toLogical(schema, value)
                }

            TO_CONNECT_LOGICAL_CONVERTERS[Time.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is Int) {
                        throw DataException(
                            "Invalid type for Time, underlying representation should be int32 but was " +
                                value!!.javaClass,
                        )
                    }
                    Time.toLogical(schema, value)
                }

            TO_CONNECT_LOGICAL_CONVERTERS[Timestamp.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is Long) {
                        throw DataException(
                            "Invalid type for Timestamp, underlying representation should be int64 but was " +
                                value!!.javaClass,
                        )
                    }
                    Timestamp.toLogical(schema, value)
                }

            TO_AVRO_LOGICAL_CONVERTERS[Decimal.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is BigDecimal) {
                        throw DataException("Invalid type for Decimal, expected BigDecimal but was " + value!!.javaClass)
                    }
                    Decimal.fromLogical(schema, value)
                }

            TO_AVRO_LOGICAL_CONVERTERS[Date.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is java.util.Date) {
                        throw DataException("Invalid type for Date, expected Date but was " + value!!.javaClass)
                    }
                    Date.fromLogical(schema, value)
                }

            TO_AVRO_LOGICAL_CONVERTERS[Time.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is java.util.Date) {
                        throw DataException("Invalid type for Time, expected Date but was " + value!!.javaClass)
                    }
                    Time.fromLogical(schema, value)
                }

            TO_AVRO_LOGICAL_CONVERTERS[Timestamp.LOGICAL_NAME] =
                LogicalTypeConverter { schema, value ->
                    if (value !is java.util.Date) {
                        throw DataException("Invalid type for Timestamp, expected Date but was " + value!!.javaClass)
                    }
                    Timestamp.fromLogical(schema, value)
                }
        }

        /**
         * Convert from Connect data format to Avro. This version assumes the Avro schema has already
         * been converted and makes the use of NonRecordContainer optional
         *
         * @param schema                         the Connect schema
         * @param avroSchema                     the corresponding
         * @param logicalValue                   the Connect data to convert, which may be a value for
         *                                       a logical type
         * @param requireContainer               if true, wrap primitives, maps, and arrays in a
         *                                       NonRecordContainer before returning them
         * @param requireSchemalessContainerNull if true, use a container representation of null because
         *                                       this is part of struct/array/map and we cannot represent
         *                                       nulls as true null because Anything cannot be a union
         *                                       type; otherwise, this is a top-level value and can return
         *                                       null
         * @return the converted data
         */
        private fun fromConnectData(
            schema: Schema?,
            avroSchema: ApacheAvroSchema,
            logicalValue: Any?,
            requireContainer: Boolean,
            requireSchemalessContainerNull: Boolean,
            enhancedSchemaSupport: Boolean,
        ): Any? {
            val schemaType =
                if (schema != null) schema.type() else schemaTypeForSchemalessJavaType(logicalValue)
            if (schemaType == null) {
                // Schemaless null data since schema is null and we got a null schema type from the value
                return if (requireSchemalessContainerNull) {
                    GenericRecordBuilder(ANYTHING_SCHEMA).build()
                } else {
                    null
                }
            }

            validateSchemaValue(schema, logicalValue)

            if (logicalValue == null) {
                // But if this is schemaless, we may not be able to return null directly
                return if (schema == null && requireSchemalessContainerNull) {
                    GenericRecordBuilder(ANYTHING_SCHEMA).build()
                } else {
                    null
                }
            }

            // If this is a logical type, convert it from the convenient Java type to the underlying
            // serializeable format
            var value: Any? = logicalValue
            if (schema != null && schema.name() != null) {
                val logicalConverter = TO_AVRO_LOGICAL_CONVERTERS[schema.name()]
                if (logicalConverter != null) {
                    value = logicalConverter.convert(schema, logicalValue)
                }
            }

            try {
                when (schemaType) {
                    Schema.Type.INT8 -> {
                        val byteValue = value as Byte? // Check for correct type
                        val convertedByteValue = byteValue?.toInt()
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, convertedByteValue, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.INT16 -> {
                        val shortValue = value as Short? // Check for correct type
                        val convertedShortValue = shortValue?.toInt()
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, convertedShortValue, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.INT32 -> {
                        val intValue = value as Int? // Check for correct type
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_INT_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.INT64 -> {
                        val longValue = value as Long? // Check for correct type
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_LONG_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.FLOAT32 -> {
                        val floatValue = value as Float? // Check for correct type
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_FLOAT_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.FLOAT64 -> {
                        val doubleValue = value as Double? // Check for correct type
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_DOUBLE_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.BOOLEAN -> {
                        val boolValue = value as Boolean? // Check for correct type
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_BOOLEAN_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.STRING -> {
                        if (enhancedSchemaSupport &&
                            schema != null &&
                            schema.parameters() != null &&
                            schema.parameters().containsKey(AVRO_TYPE_ENUM)
                        ) {
                            val enumSchemaName = schema.parameters()[AVRO_TYPE_ENUM]
                            val enumSchema: ApacheAvroSchema =
                                if (avroSchema.type == ApacheAvroSchema.Type.UNION) {
                                    val enumIndex = avroSchema.getIndexNamed(enumSchemaName)
                                    avroSchema.types[enumIndex]
                                } else {
                                    avroSchema
                                }
                            value = GenericData.EnumSymbol(enumSchema, value as String?)
                        } else {
                            val stringValue = value as String? // Check for correct type
                        }
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, value, ANYTHING_SCHEMA_STRING_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.BYTES -> {
                        val bytesValue = if (value is ByteArray) ByteBuffer.wrap(value) else value as ByteBuffer?
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, bytesValue, ANYTHING_SCHEMA_BYTES_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.ARRAY -> {
                        val list = value as Collection<Any?>
                        val converted: MutableList<Any?> = ArrayList(list.size)
                        val elementSchema = schema?.valueSchema()
                        val underlyingAvroSchema =
                            avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema)
                        val elementAvroSchema =
                            if (schema != null) underlyingAvroSchema.elementType else ANYTHING_SCHEMA
                        for (element in list) {
                            converted.add(
                                fromConnectData(
                                    elementSchema,
                                    elementAvroSchema,
                                    element,
                                    false,
                                    true,
                                    enhancedSchemaSupport,
                                ),
                            )
                        }
                        return maybeAddContainer(
                            avroSchema,
                            maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_ARRAY_FIELD),
                            requireContainer,
                        )
                    }

                    Schema.Type.MAP -> {
                        val map = value as Map<Any?, Any?>
                        val underlyingAvroSchema: ApacheAvroSchema
                        if (schema != null &&
                            schema.keySchema().type() == Schema.Type.STRING &&
                            !schema.keySchema().isOptional
                        ) {
                            underlyingAvroSchema = avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema)
                            val converted: MutableMap<String, Any?> = HashMap()
                            for (entry in map.entries) {
                                // Key is a String, no conversion needed
                                val convertedValue =
                                    fromConnectData(
                                        schema.valueSchema(),
                                        underlyingAvroSchema.valueType,
                                        entry.value,
                                        false,
                                        true,
                                        enhancedSchemaSupport,
                                    )
                                converted[entry.key as String] = convertedValue
                            }
                            return maybeAddContainer(avroSchema, converted, requireContainer)
                        } else {
                            val converted: MutableList<GenericRecord> = ArrayList(map.size)
                            underlyingAvroSchema = avroSchemaForUnderlyingMapEntryType(schema, avroSchema)
                            val elementSchema =
                                if (schema != null) {
                                    underlyingAvroSchema.elementType
                                } else {
                                    ANYTHING_SCHEMA_MAP_ELEMENT
                                }
                            val avroKeySchema = elementSchema.getField(KEY_FIELD).schema()
                            val avroValueSchema = elementSchema.getField(VALUE_FIELD).schema()
                            for (entry in map.entries) {
                                val keyConverted =
                                    fromConnectData(
                                        schema?.keySchema(),
                                        avroKeySchema,
                                        entry.key,
                                        false,
                                        true,
                                        enhancedSchemaSupport,
                                    )
                                val valueConverted =
                                    fromConnectData(
                                        schema?.valueSchema(),
                                        avroValueSchema,
                                        entry.value,
                                        false,
                                        true,
                                        enhancedSchemaSupport,
                                    )
                                converted.add(
                                    GenericRecordBuilder(elementSchema)
                                        .set(KEY_FIELD, keyConverted)
                                        .set(VALUE_FIELD, valueConverted)
                                        .build(),
                                )
                            }
                            return maybeAddContainer(
                                avroSchema,
                                maybeWrapSchemaless(schema, converted, ANYTHING_SCHEMA_MAP_FIELD),
                                requireContainer,
                            )
                        }
                    }

                    Schema.Type.STRUCT -> {
                        val struct = value as Struct
                        if (struct.schema() != schema) {
                            throw DataException("Mismatching struct schema")
                        }
                        // This handles the inverting of a union which is held as a struct, where each field is
                        // one of the union types.
                        if (AVRO_TYPE_UNION == schema!!.name()) {
                            for (field in schema.fields()) {
                                val fieldValue = struct.get(field)
                                if (fieldValue != null) {
                                    return fromConnectData(
                                        field.schema(),
                                        avroSchema,
                                        fieldValue,
                                        false,
                                        true,
                                        enhancedSchemaSupport,
                                    )
                                }
                            }
                            return fromConnectData(schema, avroSchema, null, false, true, enhancedSchemaSupport)
                        } else {
                            val underlyingAvroSchema =
                                avroSchemaForUnderlyingTypeIfOptional(schema, avroSchema)
                            val convertedBuilder = GenericRecordBuilder(underlyingAvroSchema)
                            for (field in schema.fields()) {
                                val theField = underlyingAvroSchema.getField(field.name())
                                val fieldAvroSchema = theField.schema()
                                convertedBuilder.set(
                                    field.name(),
                                    fromConnectData(
                                        field.schema(),
                                        fieldAvroSchema,
                                        struct.get(field),
                                        false,
                                        true,
                                        enhancedSchemaSupport,
                                    ),
                                )
                            }
                            return convertedBuilder.build()
                        }
                    }

                    else -> throw DataException("Unknown schema type: " + schema!!.type())
                }
            } catch (e: ClassCastException) {
                throw DataException("Invalid type for " + schema!!.type() + ": " + value!!.javaClass)
            }
        }

        /**
         * MapEntry types in connect Schemas are represented as Arrays of record.
         * Return the array type from the union instead of the union itself.
         */
        private fun avroSchemaForUnderlyingMapEntryType(
            schema: Schema?,
            avroSchema: ApacheAvroSchema,
        ): ApacheAvroSchema {
            if (schema != null && schema.isOptional) {
                if (avroSchema.type == ApacheAvroSchema.Type.UNION) {
                    for (typeSchema in avroSchema.types) {
                        if (typeSchema.type != ApacheAvroSchema.Type.NULL &&
                            Schema.Type.ARRAY.getName() == typeSchema.type.getName()
                        ) {
                            return typeSchema
                        }
                    }
                } else {
                    throw DataException(
                        "An optional schema should have an Avro Union type, not " + schema.type(),
                    )
                }
            }
            return avroSchema
        }

        private fun crossReferenceSchemaNames(
            schema: Schema,
            avroSchema: ApacheAvroSchema,
        ): Boolean = Objects.equals(avroSchema.fullName, schema.name()) ||
            Objects.equals(avroSchema.type.getName(), schema.type().getName()) ||
            (schema.name() == null && avroSchema.fullName == DEFAULT_SCHEMA_FULL_NAME)

        /**
         * Connect optional fields are represented as a unions (null & type) in Avro
         * Return the Avro schema of the actual type in the Union (instead of the union itself)
         */
        private fun avroSchemaForUnderlyingTypeIfOptional(
            schema: Schema?,
            avroSchema: ApacheAvroSchema,
        ): ApacheAvroSchema {
            if (schema != null && schema.isOptional) {
                if (avroSchema.type == ApacheAvroSchema.Type.UNION) {
                    for (typeSchema in avroSchema.types) {
                        if (typeSchema.type != ApacheAvroSchema.Type.NULL &&
                            crossReferenceSchemaNames(schema, typeSchema)
                        ) {
                            return typeSchema
                        }
                    }
                } else {
                    throw DataException(
                        "An optinal schema should have an Avro Union type, not " + schema.type(),
                    )
                }
            }
            return avroSchema
        }

        private fun schemaTypeForSchemalessJavaType(value: Any?): Schema.Type? = when (value) {
            null -> null
            is Byte -> Schema.Type.INT8
            is Short -> Schema.Type.INT16
            is Int -> Schema.Type.INT32
            is Long -> Schema.Type.INT64
            is Float -> Schema.Type.FLOAT32
            is Double -> Schema.Type.FLOAT64
            is Boolean -> Schema.Type.BOOLEAN
            is String -> Schema.Type.STRING
            is Collection<*> -> Schema.Type.ARRAY
            is Map<*, *> -> Schema.Type.MAP
            else -> throw DataException("Unknown Java type for schemaless data: " + value.javaClass)
        }

        private fun maybeAddContainer(
            avroSchema: ApacheAvroSchema,
            value: Any?,
            wrap: Boolean,
        ): Any? = if (wrap) NonRecordContainer(avroSchema, value) else value

        private fun maybeWrapSchemaless(
            schema: Schema?,
            value: Any?,
            typeField: String,
        ): Any? {
            if (schema != null) {
                return value
            }

            val builder = GenericRecordBuilder(ANYTHING_SCHEMA)
            if (value != null) {
                builder.set(typeField, value)
            }
            return builder.build()
        }

        private fun toAvroLogical(
            schema: Schema?,
            value: Any?,
        ): Any? {
            if (schema != null && schema.name() != null) {
                val logicalConverter = TO_AVRO_LOGICAL_CONVERTERS[schema.name()]
                if (logicalConverter != null && value != null) {
                    return logicalConverter.convert(schema, value)
                }
            }
            return value
        }

        private fun toConnectLogical(
            schema: Schema?,
            value: Any?,
        ): Any? {
            if (schema != null && schema.name() != null) {
                val logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS[schema.name()]
                if (logicalConverter != null && value != null) {
                    return logicalConverter.convert(schema, value)
                }
            }
            return value
        }

        // Convert default values from Connect data format to Avro's format, which is an
        // org.codehaus.jackson.JsonNode. The default value is provided as an argument because even
        // though you can get a default value from the schema, default values for complex structures need
        // to perform the same translation but those defaults will be part of the original top-level
        // (complex type) default value, not part of the child schema.
        private fun defaultValueFromConnect(
            schema: Schema,
            value: Any?,
        ): JsonNode {
            try {
                // If this is a logical type, convert it from the convenient Java type to the underlying
                // serializable format
                val defaultVal = toAvroLogical(schema, value)

                when (schema.type()) {
                    Schema.Type.INT8 -> return JsonNodeFactory.instance.numberNode((defaultVal as Byte).toInt())
                    Schema.Type.INT16 -> return JsonNodeFactory.instance.numberNode((defaultVal as Short).toInt())
                    Schema.Type.INT32 -> return JsonNodeFactory.instance.numberNode(defaultVal as Int)
                    Schema.Type.INT64 -> return JsonNodeFactory.instance.numberNode(defaultVal as Long)
                    Schema.Type.FLOAT32 -> return JsonNodeFactory.instance.numberNode(defaultVal as Float)
                    Schema.Type.FLOAT64 -> return JsonNodeFactory.instance.numberNode(defaultVal as Double)
                    Schema.Type.BOOLEAN -> return JsonNodeFactory.instance.booleanNode(defaultVal as Boolean)
                    Schema.Type.STRING -> return JsonNodeFactory.instance.textNode(defaultVal as String?)
                    Schema.Type.BYTES ->
                        return if (defaultVal is ByteArray) {
                            JsonNodeFactory.instance.textNode(String(defaultVal, StandardCharsets.ISO_8859_1))
                        } else {
                            JsonNodeFactory.instance.textNode(
                                String((defaultVal as ByteBuffer).array(), StandardCharsets.ISO_8859_1),
                            )
                        }

                    Schema.Type.ARRAY -> {
                        val array = JsonNodeFactory.instance.arrayNode()
                        for (elem in defaultVal as Collection<Any?>) {
                            array.add(defaultValueFromConnect(schema.valueSchema(), elem))
                        }
                        return array
                    }

                    Schema.Type.MAP ->
                        if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional) {
                            val node = JsonNodeFactory.instance.objectNode()
                            for (entry in (defaultVal as Map<String, Any?>).entries) {
                                val entryDef = defaultValueFromConnect(schema.valueSchema(), entry.value)
                                node.put(entry.key, entryDef)
                            }
                            return node
                        } else {
                            val array = JsonNodeFactory.instance.arrayNode()
                            for (entry in (defaultVal as Map<Any?, Any?>).entries) {
                                val keyDefault = defaultValueFromConnect(schema.keySchema(), entry.key)
                                val valDefault = defaultValueFromConnect(schema.valueSchema(), entry.value)
                                val jsonEntry = JsonNodeFactory.instance.arrayNode()
                                jsonEntry.add(keyDefault)
                                jsonEntry.add(valDefault)
                                array.add(jsonEntry)
                            }
                            return array
                        }

                    Schema.Type.STRUCT -> {
                        val node = JsonNodeFactory.instance.objectNode()
                        val struct = defaultVal as Struct
                        for (field in schema.fields()) {
                            val fieldDef = defaultValueFromConnect(field.schema(), struct.get(field))
                            node.put(field.name(), fieldDef)
                        }
                        return node
                    }

                    else -> throw DataException("Unknown schema type:" + schema.type())
                }
            } catch (e: ClassCastException) {
                throw DataException(
                    "Invalid type used for default value of " +
                        schema.type() +
                        " field: " +
                        schema.defaultValue().javaClass,
                )
            }
        }

        private fun parametersFromConnect(params: Map<String, String>): JsonNode {
            val result = JsonNodeFactory.instance.objectNode()
            for (entry in params.entries) {
                if (entry.key != AVRO_FIELD_DEFAULT_FLAG_PROP) {
                    result.put(entry.key, entry.value)
                }
            }
            return result
        }

        @Throws(DataException::class)
        private fun validateSchemaValue(
            schema: Schema?,
            value: Any?,
        ) {
            if ((value == null || value === JsonProperties.NULL_VALUE) && schema != null && !schema.isOptional) {
                throw DataException("Found null value for non-optional schema")
            }
        }

        private fun isEnumSchema(schema: Schema): Boolean = schema.type() == Schema.Type.STRING &&
            schema.parameters() != null &&
            schema.parameters().containsKey(AVRO_TYPE_ENUM)

        private fun isInstanceOfAvroSchemaTypeForSimpleSchema(
            fieldSchema: Schema,
            value: Any?,
        ): Boolean {
            val classes = SIMPLE_AVRO_SCHEMA_TYPES[fieldSchema.type()] ?: return false
            for (type in classes) {
                if (type.isInstance(value)) {
                    return true
                }
            }
            return false
        }

        /**
         * Split a full dotted-syntax name into a namespace and a single-component name.
         */
        private fun splitName(fullName: String): Array<String?> {
            val result = arrayOfNulls<String>(2)
            val indexLastDot = fullName.lastIndexOf('.')
            if (indexLastDot >= 0) {
                result[0] = fullName.substring(0, indexLastDot)
                result[1] = fullName.substring(indexLastDot + 1)
            } else {
                result[0] = null
                result[1] = fullName
            }
            return result
        }

        @JvmStatic
        fun nonOptional(schema: Schema): Schema = ConnectSchema(
            schema.type(),
            false,
            schema.defaultValue(),
            schema.name(),
            schema.version(),
            schema.doc(),
            schema.parameters(),
            fields(schema),
            keySchema(schema),
            valueSchema(schema),
        )

        @JvmStatic
        fun fields(schema: Schema): List<Field>? {
            val type = schema.type()
            return if (Schema.Type.STRUCT == type) schema.fields() else null
        }

        @JvmStatic
        fun keySchema(schema: Schema): Schema? {
            val type = schema.type()
            return if (Schema.Type.MAP == type) schema.keySchema() else null
        }

        @JvmStatic
        fun valueSchema(schema: Schema): Schema? {
            val type = schema.type()
            return if (Schema.Type.MAP == type || Schema.Type.ARRAY == type) schema.valueSchema() else null
        }

        private fun fieldListEquals(
            one: List<Field>?,
            two: List<Field>?,
            cache: MutableMap<SchemaPair, Boolean>,
        ): Boolean {
            if (one === two) {
                return true
            } else if (one == null || two == null) {
                return false
            } else {
                val itOne = one.listIterator()
                val itTwo = two.listIterator()
                while (itOne.hasNext() && itTwo.hasNext()) {
                    if (!fieldEquals(itOne.next(), itTwo.next(), cache)) {
                        return false
                    }
                }
                return itOne.hasNext() == itTwo.hasNext()
            }
        }

        private fun fieldEquals(
            one: Field?,
            two: Field?,
            cache: MutableMap<SchemaPair, Boolean>,
        ): Boolean = if (one === two) {
            true
        } else if (one == null || two == null) {
            false
        } else {
            one.javaClass == two.javaClass &&
                Objects.equals(one.index(), two.index()) &&
                Objects.equals(one.name(), two.name()) &&
                schemaEquals(one.schema(), two.schema(), cache)
        }

        private fun schemaEquals(
            src: Schema?,
            that: Schema?,
        ): Boolean = schemaEquals(src, that, HashMap())

        private fun schemaEquals(
            src: Schema?,
            that: Schema?,
            cache: MutableMap<SchemaPair, Boolean>,
        ): Boolean {
            if (src === that) {
                return true
            } else if (src == null || that == null) {
                return false
            }

            // Add a temporary value to the cache to avoid cycles. As long as we recurse only at the end of
            // the method, we can safely default to true here. The cache is updated at the end of the method
            // with the actual comparison result.
            val sp = SchemaPair(src, that)
            val cacheHit = cache.putIfAbsent(sp, true)
            if (cacheHit != null) {
                return cacheHit
            }

            var equals =
                Objects.equals(src.isOptional, that.isOptional) &&
                    Objects.equals(src.version(), that.version()) &&
                    Objects.equals(src.name(), that.name()) &&
                    Objects.equals(src.doc(), that.doc()) &&
                    Objects.equals(src.type(), that.type()) &&
                    Objects.deepEquals(src.defaultValue(), that.defaultValue()) &&
                    Objects.equals(src.parameters(), that.parameters())

            when (src.type()) {
                Schema.Type.STRUCT -> equals = equals && fieldListEquals(src.fields(), that.fields(), cache)
                Schema.Type.ARRAY -> equals = equals && schemaEquals(src.valueSchema(), that.valueSchema(), cache)
                Schema.Type.MAP ->
                    equals =
                        equals &&
                        schemaEquals(src.valueSchema(), that.valueSchema(), cache) &&
                        schemaEquals(src.keySchema(), that.keySchema(), cache)

                else -> Unit
            }
            cache[sp] = equals
            return equals
        }
    }
}
