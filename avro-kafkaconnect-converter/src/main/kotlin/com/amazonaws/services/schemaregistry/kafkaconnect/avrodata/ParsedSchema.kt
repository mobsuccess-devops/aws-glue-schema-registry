/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

import java.util.Collections
import java.util.Objects

/**
 * A parsed schema.
 */
interface ParsedSchema {
    /** Returns the schema type. */
    fun schemaType(): String

    /** Returns a name for the schema, or null. */
    fun name(): String?

    /** Returns a canonical string representation of the schema. */
    fun canonicalString(): String

    /** Returns a formatted string according to a type-specific format. */
    fun formattedString(format: String?): String {
        if (format == null || format.trim().isEmpty()) {
            return canonicalString()
        }
        throw IllegalArgumentException("Format not supported: $format")
    }

    /**
     * Validates the schema and ensures all references are resolved properly.
     * Throws an exception if the schema is not valid.
     */
    fun validate() {
        // No-op by default.
    }

    /**
     * Checks the backward compatibility between this schema and the specified schema. Custom
     * providers may modify this schema during the check to make it compatible.
     *
     * @return an empty list when compatible, otherwise the list of error messages
     */
    fun isBackwardCompatible(previousSchema: ParsedSchema): List<String>

    /**
     * Checks the compatibility between this schema and the specified schemas, given in
     * chronological order. Custom providers may modify this schema during the check.
     *
     * @return an empty list when compatible, otherwise the list of error messages
     */
    fun isCompatible(
        level: CompatibilityLevel,
        previousSchemas: List<ParsedSchema>,
    ): List<String> {
        for (previousSchema in previousSchemas) {
            if (schemaType() != previousSchema.schemaType()) {
                return Collections.singletonList("Incompatible because of different schema type")
            }
        }
        return CompatibilityChecker.checker(level).isCompatible(this, previousSchemas)
    }

    /** Returns the underlying raw representation of the schema. */
    fun rawSchema(): Any?

    /** Returns whether the underlying raw representations are equal. */
    fun deepEquals(schema: ParsedSchema): Boolean = Objects.equals(rawSchema(), schema.rawSchema())
}
