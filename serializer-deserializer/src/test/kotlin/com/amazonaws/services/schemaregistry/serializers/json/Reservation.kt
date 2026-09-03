/*
 * Copyright 2026 Mobsuccess.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.serializers.json

import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString
import java.time.Instant
import java.time.LocalDate

/**
 * A POJO holding `java.time` values, which neither Jackson nor the schema generator handles
 * until a `java.time` module is registered on the mapper.
 */
@JsonSchemaInject(
    strings = [
        JsonSchemaString(
            path = "className",
            value = "com.amazonaws.services.schemaregistry.serializers.json.Reservation",
        ),
    ],
)
class Reservation {
    var reference: String? = null
    var checkIn: LocalDate? = null
    var bookedAt: Instant? = null

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Reservation) return false
        return reference == other.reference && checkIn == other.checkIn && bookedAt == other.bookedAt
    }

    override fun hashCode(): Int {
        var result = reference?.hashCode() ?: 0
        result = 31 * result + (checkIn?.hashCode() ?: 0)
        result = 31 * result + (bookedAt?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String = "Reservation(reference=$reference, checkIn=$checkIn, bookedAt=$bookedAt)"

    companion object {
        @JvmStatic
        fun of(
            reference: String,
            checkIn: LocalDate,
            bookedAt: Instant,
        ): Reservation = Reservation().apply {
            this.reference = reference
            this.checkIn = checkIn
            this.bookedAt = bookedAt
        }
    }
}
