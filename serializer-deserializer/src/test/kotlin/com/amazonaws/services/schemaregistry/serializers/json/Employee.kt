package com.amazonaws.services.schemaregistry.serializers.json

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString

@JsonSchemaInject(
    strings = [JsonSchemaString(path = "className", value = "wrong.class.name")],
)
class Employee(
    @JsonProperty
    private val name: String? = null,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Employee) return false
        return name == other.name
    }

    override fun hashCode(): Int = name?.hashCode() ?: 0

    /** Mirrors the fluent API Lombok generated. */
    class EmployeeBuilder {
        private var name: String? = null

        fun name(name: String?): EmployeeBuilder = apply { this.name = name }

        fun build(): Employee = Employee(name)
    }

    companion object {
        @JvmStatic
        fun builder(): EmployeeBuilder = EmployeeBuilder()
    }
}
