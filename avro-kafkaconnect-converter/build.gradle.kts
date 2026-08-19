plugins {
    id("gsr.shaded-conventions")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)

    testImplementation(libs.junit4)
    testRuntimeOnly(libs.junit.vintageEngine)
    testImplementation(libs.powermock.reflect)
    testImplementation(libs.podam)
}

avro {
    stringType.set("String")
}
