plugins {
    id("gsr.distribution-conventions")
}

coverage {
    minimumInstructionCoverage.set(0.86)
    minimumBranchCoverage.set(0.77)
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
