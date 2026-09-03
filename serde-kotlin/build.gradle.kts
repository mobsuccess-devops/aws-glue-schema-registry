plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

coverage {
    minimumInstructionCoverage.set(0.85)
    minimumBranchCoverage.set(0.55)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(project(":schema-registry-kafkastreams-serde"))
    api(libs.kafka.clients)

    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.jackson.databind)
    testImplementation(libs.jackson.jsr310)
}
