plugins {
    id("gsr.distribution-conventions")
    id("com.github.davidmc24.gradle.plugin.avro")
}

coverage {
    minimumInstructionCoverage.set(0.79)
    minimumBranchCoverage.set(0.74)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)

    implementation(platform(libs.jackson.bom))
    implementation(libs.aws.sts)
    implementation(libs.aws.urlConnectionClient)
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)

    testImplementation(libs.junit4)
    testRuntimeOnly(libs.junit.vintageEngine)
    testImplementation(libs.powermock.reflect)
    testImplementation(libs.podam)
}

avro {
    stringType.set("String")
}
