/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

/**
 * A SchemaValidator has one method, which validates that a [ParsedSchema] is **compatible** with
 * the other schemas provided. What makes one schema compatible with another is not part of the
 * interface contract.
 */
fun interface SchemaValidator {
    /**
     * Validate one schema against others, ordered from most recent to oldest where a natural
     * chronological order exists, so that a validator may choose to check only the most recent.
     *
     * @return list of error messages, otherwise an empty list
     */
    fun validate(
        toValidate: ParsedSchema,
        existing: Iterable<ParsedSchema>,
    ): List<String>
}
