plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))
    api(project(":schema-registry-kafkastreams-serde"))
    api(libs.kafka.clients)

    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.jackson.databind)
}
