/*
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 */

package org.apache.avro

import java.io.IOException
import java.io.StringWriter

object Schemas {
    @JvmStatic
    @JvmOverloads
    fun toString(
        schema: Schema,
        schemas: Collection<Schema>?,
        pretty: Boolean = false,
    ): String {
        try {
            val writer = StringWriter()
            val gen = Schema.FACTORY.createGenerator(writer)
            if (pretty) {
                gen.useDefaultPrettyPrinter()
            }
            val names = Schema.Names()
            schemas?.forEach { names.add(it) }
            schema.toJson(names, gen)
            gen.flush()
            return writer.toString()
        } catch (e: IOException) {
            throw AvroRuntimeException(e)
        }
    }
}
