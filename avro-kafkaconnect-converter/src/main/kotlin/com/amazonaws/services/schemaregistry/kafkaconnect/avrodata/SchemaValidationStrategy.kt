/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

/**
 * An interface for validating the compatibility of a single schema against another.
 *
 * What makes one schema compatible with another is not defined by the contract.
 */
fun interface SchemaValidationStrategy {
    /**
     * Validates that one schema is compatible with another.
     *
     * @return list of error messages, otherwise an empty list
     */
    fun validate(
        toValidate: ParsedSchema,
        existing: ParsedSchema,
    ): List<String>
}
