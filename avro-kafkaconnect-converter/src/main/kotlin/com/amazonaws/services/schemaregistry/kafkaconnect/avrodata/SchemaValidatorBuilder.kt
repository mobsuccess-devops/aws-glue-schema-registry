/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

/**
 * A builder for creating SchemaValidators.
 */
class SchemaValidatorBuilder {
    private var strategy: SchemaValidationStrategy? = null

    /**
     * Use a strategy that validates that a schema can be used to read existing schemas according
     * to the JSON default schema resolution.
     */
    fun canReadStrategy(): SchemaValidatorBuilder = apply {
        strategy = SchemaValidationStrategy { toValidate, existing -> toValidate.isBackwardCompatible(existing) }
    }

    /**
     * Use a strategy that validates that a schema can be read by existing schemas according to the
     * JSON default schema resolution.
     */
    fun canBeReadStrategy(): SchemaValidatorBuilder = apply {
        strategy = SchemaValidationStrategy { toValidate, existing -> existing.isBackwardCompatible(toValidate) }
    }

    /**
     * Use a strategy that validates that a schema can read existing schemas and vice-versa,
     * according to the JSON default schema resolution.
     */
    fun mutualReadStrategy(): SchemaValidatorBuilder = apply {
        strategy =
            SchemaValidationStrategy { toValidate, existing ->
                existing.isBackwardCompatible(toValidate) + toValidate.isBackwardCompatible(existing)
            }
    }

    fun validateLatest(): SchemaValidator {
        valid()
        return SchemaValidator { toValidate, existing ->
            existing.firstOrNull()?.let { strategy!!.validate(toValidate, it) } ?: emptyList()
        }
    }

    fun validateAll(): SchemaValidator {
        valid()
        return SchemaValidator { toValidate, existing ->
            existing
                .asSequence()
                .map { strategy!!.validate(toValidate, it) }
                .firstOrNull { it.isNotEmpty() }
                ?: emptyList()
        }
    }

    private fun valid() {
        if (strategy == null) {
            throw RuntimeException("SchemaValidationStrategy not specified in builder")
        }
    }
}
