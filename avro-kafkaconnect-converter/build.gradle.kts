plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.shadow)
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

tasks.shadowJar {
    archiveClassifier.set("")
}

tasks.jar {
    enabled = false
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
}
