plugins {
    id("gsr.shaded-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)
    api(libs.kafka.connectJson)
    api(libs.everit.jsonSchema)
}

