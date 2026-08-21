plugins {
    id("gsr.shaded-conventions")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.kafka.connectApi)
    api(libs.protobuf.java)
    api(libs.protoGoogleCommon)

    implementation(libs.protobuf.javaUtil)

    runtimeOnly(libs.apicurio.protobufUtils) {
        exclude(group = "com.ibm.icu", module = "icu4j")
    }
}

protobuf {
    protoc {
        artifact = libs.protobuf.protoc.get().toString()
    }
}
