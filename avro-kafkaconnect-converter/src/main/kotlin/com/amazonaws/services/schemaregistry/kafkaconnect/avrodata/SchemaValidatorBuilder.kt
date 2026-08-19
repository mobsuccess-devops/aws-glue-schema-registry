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
        strategy =
            object : SchemaValidationStrategy {
                override fun validate(
                    toValidate: ParsedSchema,
                    existing: ParsedSchema,
                ): List<String> = toValidate.isBackwardCompatible(existing)
            }
    }

    /**
     * Use a strategy that validates that a schema can be read by existing schemas according to the
     * JSON default schema resolution.
     */
    fun canBeReadStrategy(): SchemaValidatorBuilder = apply {
        strategy =
            object : SchemaValidationStrategy {
                override fun validate(
                    toValidate: ParsedSchema,
                    existing: ParsedSchema,
                ): List<String> = existing.isBackwardCompatible(toValidate)
            }
    }

    /**
     * Use a strategy that validates that a schema can read existing schemas and vice-versa,
     * according to the JSON default schema resolution.
     */
    fun mutualReadStrategy(): SchemaValidatorBuilder = apply {
        strategy =
            object : SchemaValidationStrategy {
                override fun validate(
                    toValidate: ParsedSchema,
                    existing: ParsedSchema,
                ): List<String> {
                    val result = ArrayList<String>()
                    result.addAll(existing.isBackwardCompatible(toValidate))
                    result.addAll(toValidate.isBackwardCompatible(existing))
                    return result
                }
            }
    }

    fun validateLatest(): SchemaValidator {
        valid()
        return object : SchemaValidator {
            override fun validate(
                toValidate: ParsedSchema,
                existing: Iterable<ParsedSchema>,
            ): List<String> {
                val schemas = existing.iterator()
                if (schemas.hasNext()) {
                    return strategy!!.validate(toValidate, schemas.next())
                }
                return emptyList()
            }
        }
    }

    fun validateAll(): SchemaValidator {
        valid()
        return object : SchemaValidator {
            override fun validate(
                toValidate: ParsedSchema,
                existing: Iterable<ParsedSchema>,
            ): List<String> {
                for (schema in existing) {
                    val errorMessages = strategy!!.validate(toValidate, schema)
                    if (errorMessages.isNotEmpty()) {
                        return errorMessages
                    }
                }
                return emptyList()
            }
        }
    }

    private fun valid() {
        if (strategy == null) {
            throw RuntimeException("SchemaValidationStrategy not specified in builder")
        }
    }
}
