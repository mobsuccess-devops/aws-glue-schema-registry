plugins {
    id("gsr.shaded-conventions")
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

