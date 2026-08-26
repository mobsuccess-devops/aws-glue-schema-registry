plugins {
    id("gsr.shaded-conventions")
    alias(libs.plugins.protobuf)
}

coverage {
    minimumInstructionCoverage.set(0.93)
    minimumBranchCoverage.set(0.86)
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
