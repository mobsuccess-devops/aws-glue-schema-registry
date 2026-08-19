/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

class CompatibilityChecker private constructor(
    private val validator: SchemaValidator,
) {
    // Visible for testing.
    fun isCompatible(
        newSchema: ParsedSchema,
        previousSchemas: List<ParsedSchema>,
    ): List<String> {
        // The validator checks in list order, but checks should occur in reverse chronological order.
        val previousSchemasCopy = ArrayList(previousSchemas)
        previousSchemasCopy.reverse()
        return validator.validate(newSchema, previousSchemasCopy)
    }

    companion object {
        // Check if the new schema can be used to read data produced by the previous schema
        private val BACKWARD_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().canReadStrategy().validateLatest()

        @JvmField
        val BACKWARD_CHECKER = CompatibilityChecker(BACKWARD_VALIDATOR)

        // Check if data produced by the new schema can be read by the previous schema
        private val FORWARD_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().canBeReadStrategy().validateLatest()

        @JvmField
        val FORWARD_CHECKER = CompatibilityChecker(FORWARD_VALIDATOR)

        // Check if the new schema is both forward and backward compatible with the previous schema
        private val FULL_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

        @JvmField
        val FULL_CHECKER = CompatibilityChecker(FULL_VALIDATOR)

        // Check if the new schema can be used to read data produced by all earlier schemas
        private val BACKWARD_TRANSITIVE_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().canReadStrategy().validateAll()

        @JvmField
        val BACKWARD_TRANSITIVE_CHECKER = CompatibilityChecker(BACKWARD_TRANSITIVE_VALIDATOR)

        // Check if data produced by the new schema can be read by all earlier schemas
        private val FORWARD_TRANSITIVE_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().canBeReadStrategy().validateAll()

        @JvmField
        val FORWARD_TRANSITIVE_CHECKER = CompatibilityChecker(FORWARD_TRANSITIVE_VALIDATOR)

        // Check if the new schema is both forward and backward compatible with all earlier schemas
        private val FULL_TRANSITIVE_VALIDATOR: SchemaValidator =
            SchemaValidatorBuilder().mutualReadStrategy().validateAll()

        @JvmField
        val FULL_TRANSITIVE_CHECKER = CompatibilityChecker(FULL_TRANSITIVE_VALIDATOR)

        private val NO_OP_VALIDATOR =
            object : SchemaValidator {
                override fun validate(
                    toValidate: ParsedSchema,
                    existing: Iterable<ParsedSchema>,
                ): List<String> = emptyList()
            }

        @JvmField
        val NO_OP_CHECKER = CompatibilityChecker(NO_OP_VALIDATOR)

        @JvmStatic
        fun checker(level: CompatibilityLevel): CompatibilityChecker = when (level) {
            CompatibilityLevel.NONE -> NO_OP_CHECKER
            CompatibilityLevel.BACKWARD -> BACKWARD_CHECKER
            CompatibilityLevel.BACKWARD_TRANSITIVE -> BACKWARD_TRANSITIVE_CHECKER
            CompatibilityLevel.FORWARD -> FORWARD_CHECKER
            CompatibilityLevel.FORWARD_TRANSITIVE -> FORWARD_TRANSITIVE_CHECKER
            CompatibilityLevel.FULL -> FULL_CHECKER
            CompatibilityLevel.FULL_TRANSITIVE -> FULL_TRANSITIVE_CHECKER
        }
    }
}
