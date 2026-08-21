plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(project(":schema-registry-serde"))

    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.jackson.annotations)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.mbknor.jsonSchema)
}
