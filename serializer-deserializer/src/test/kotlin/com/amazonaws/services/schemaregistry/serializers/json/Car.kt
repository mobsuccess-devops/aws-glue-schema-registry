/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDefault
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInt
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import java.util.Date
import javax.validation.constraints.Max
import javax.validation.constraints.Min

// List of annotations to help infer JSON Schema are defined by https://github.com/mbknor/mbknor-jackson-jsonSchema
@JsonSchemaDescription("This is a car")
@JsonSchemaTitle("Simple Car Schema")
// Fully qualified class name to be added to an additionally injected property
// called className for deserializer to determine which class to deserialize
// the bytes into
@JsonSchemaInject(
    strings = [
        JsonSchemaString(
            path = "className",
            value = "com.amazonaws.services.schemaregistry.serializers.json.Car",
        ),
    ],
)
class Car(
    @JsonProperty(required = true)
    private val make: String? = null,
    @JsonProperty(required = true)
    private val model: String? = null,
    @JsonSchemaDefault("true")
    @JsonProperty
    @JvmField
    var used: Boolean = false,
    @JsonSchemaInject(ints = [JsonSchemaInt(path = "multipleOf", value = 1000)])
    @Max(200000)
    @JsonProperty
    private val miles: Int = 0,
    @Min(2000)
    @JsonProperty
    private val year: Int = 0,
    @JsonProperty
    private val purchaseDate: Date? = null,
    @JsonProperty
    @JsonFormat(shape = JsonFormat.Shape.NUMBER)
    private val listedDate: Date? = null,
    @JsonProperty
    private val owners: Array<String>? = null,
    @JsonProperty
    private val serviceChecks: Collection<Float>? = null,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Car) return false
        return make == other.make &&
            model == other.model &&
            used == other.used &&
            miles == other.miles &&
            year == other.year &&
            purchaseDate == other.purchaseDate &&
            listedDate == other.listedDate &&
            owners.contentEquals(other.owners) &&
            serviceChecks == other.serviceChecks
    }

    override fun hashCode(): Int {
        var result = make?.hashCode() ?: 0
        result = 31 * result + (model?.hashCode() ?: 0)
        result = 31 * result + used.hashCode()
        result = 31 * result + miles
        result = 31 * result + year
        result = 31 * result + (purchaseDate?.hashCode() ?: 0)
        result = 31 * result + (listedDate?.hashCode() ?: 0)
        result = 31 * result + owners.contentHashCode()
        result = 31 * result + (serviceChecks?.hashCode() ?: 0)
        return result
    }

    /** Mirrors the fluent API Lombok generated. */
    class CarBuilder {
        private var make: String? = null
        private var model: String? = null
        private var used: Boolean = false
        private var miles: Int = 0
        private var year: Int = 0
        private var purchaseDate: Date? = null
        private var listedDate: Date? = null
        private var owners: Array<String>? = null
        private var serviceChecks: Collection<Float>? = null

        fun make(make: String?): CarBuilder = apply { this.make = make }

        fun model(model: String?): CarBuilder = apply { this.model = model }

        fun used(used: Boolean): CarBuilder = apply { this.used = used }

        fun miles(miles: Int): CarBuilder = apply { this.miles = miles }

        fun year(year: Int): CarBuilder = apply { this.year = year }

        fun purchaseDate(purchaseDate: Date?): CarBuilder = apply { this.purchaseDate = purchaseDate }

        fun listedDate(listedDate: Date?): CarBuilder = apply { this.listedDate = listedDate }

        fun owners(owners: Array<String>?): CarBuilder = apply { this.owners = owners }

        fun serviceChecks(serviceChecks: Collection<Float>?): CarBuilder = apply {
            this.serviceChecks = serviceChecks
        }

        fun build(): Car = Car(make, model, used, miles, year, purchaseDate, listedDate, owners, serviceChecks)
    }

    companion object {
        @JvmStatic
        fun builder(): CarBuilder = CarBuilder()
    }
}
