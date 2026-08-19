/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.avrodata

enum class CompatibilityLevel {
    NONE,
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE,
    ;

    companion object {
        // The original carried a redundant `name` field always equal to the constant name; Kotlin
        // cannot redeclare it, and Enum.name already provides the same value.
        @JvmStatic
        fun forName(name: String?): CompatibilityLevel? {
            if (name == null) {
                return null
            }
            return entries.firstOrNull { it.name == name.uppercase() }
        }
    }
}
