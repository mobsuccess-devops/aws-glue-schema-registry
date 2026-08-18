plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.shadow)
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)
    api(libs.protobuf.java)
    api(libs.protobuf.javaUtil)
    api(libs.apicurio.protobufUtils)
}

protobuf {
    protoc {
        artifact = libs.protobuf.protoc.get().toString()
    }
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
