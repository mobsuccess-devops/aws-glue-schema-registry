plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

coverage {
    minimumInstructionCoverage.set(0.94)
    minimumBranchCoverage.set(0.97)
}

dependencies {
    // The original pom pointed at schema-registry-serde published on Maven Central
    // (2.0.0 for compile, 1.0.2 for test) rather than at the neighbouring module; the
    // internal dependency is restored, being the only coherent one in a multi-module build.
    api(project(":schema-registry-serde"))
    api(libs.flink.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    compileOnly(libs.flink.streamingJava) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }

    testImplementation(libs.flink.streamingJava) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    testImplementation(libs.junit4)
    testImplementation(libs.hamcrest)
}
