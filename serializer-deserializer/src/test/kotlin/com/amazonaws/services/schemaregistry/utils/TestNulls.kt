package com.amazonaws.services.schemaregistry.utils

/**
 * Passes a null through a parameter Kotlin declares non-nullable, so the null check that
 * fires is the callee's own — as it was when the tests handed a raw null to Java.
 */
fun <T> nullOf(): T = null as T
