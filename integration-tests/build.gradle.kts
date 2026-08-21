plugins {
    id("gsr.java-conventions")
}

// Integration test module: nothing to publish, and its tests require real AWS
// resources (they are excluded from the unit run by the convention plugin).
dependencies {
    testImplementation(project(":schema-registry-serde"))
    testImplementation(project(":schema-registry-kafkastreams-serde"))
    testImplementation(platform(libs.aws.bom))
    testImplementation(platform(libs.jackson.bom))
    testImplementation(libs.aws.kinesis)
    testImplementation(libs.aws.auth)
    testImplementation(libs.kinesis.client) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    testImplementation(libs.kinesis.producer) {
        // The exclusion the original pom carried on this dependency.
        exclude(group = "software.amazon.ion", module = "ion-java")
    }
    testImplementation(libs.kafka.clients)
    testImplementation(libs.kafka.streams)
    testImplementation(libs.localstack.utils)
    testImplementation(libs.awaitility)
    testImplementation(libs.slf4j.api)
    testImplementation(libs.avro)
    testImplementation(libs.avro.compiler)
    testImplementation(libs.avro.ipc)
    testImplementation(libs.protobuf.java)
    testImplementation(libs.log4j.api)
    testRuntimeOnly(libs.log4j.core)
    testRuntimeOnly(libs.log4j.slf4jImpl)
    testRuntimeOnly(libs.log4j.compatApi)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.hamcrest)
    testImplementation(libs.jaxb.api)
    testImplementation(libs.jackson.annotations)
    testImplementation(libs.jackson.databind)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Classes generated from serializer-deserializer/src/test/proto, consumed through
    // the test jar just as the Maven `tests` classifier did.
    testImplementation(project(path = ":schema-registry-serde", configuration = "testArtifacts"))
}
