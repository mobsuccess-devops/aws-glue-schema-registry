plugins {
    id("gsr.shaded-conventions")
}

dependencies {
    api(platform(libs.jackson.bom))
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)
    api(libs.kafka.connectJson)
    api(libs.everit.jsonSchema)
    api(libs.jackson.databind)

    implementation(libs.jackson.core)
}
