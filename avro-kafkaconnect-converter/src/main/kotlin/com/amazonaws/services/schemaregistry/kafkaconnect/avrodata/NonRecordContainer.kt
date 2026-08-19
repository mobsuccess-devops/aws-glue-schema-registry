/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.common.errors.SerializationException
import java.util.Objects

/**
 * Wrapper for all non-record types that includes the schema for the data.
 */
class NonRecordContainer(
    private val schema: Schema?,
    val value: Any?,
) : GenericContainer {
    init {
        if (schema == null) {
            throw SerializationException("Schema may not be null.")
        }
    }

    override fun getSchema(): Schema = schema!!

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as NonRecordContainer
        return schema == that.schema && value == that.value
    }

    override fun hashCode(): Int = Objects.hash(schema, value)
}
