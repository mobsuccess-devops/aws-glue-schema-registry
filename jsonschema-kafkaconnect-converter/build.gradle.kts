plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.shadow)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)
    api(libs.kafka.connectJson)
    api(libs.everit.jsonSchema)
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
