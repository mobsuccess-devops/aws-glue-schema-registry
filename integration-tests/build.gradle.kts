plugins {
    id("gsr.java-conventions")
}

// Integration test module: nothing to publish, and its tests require real AWS
// resources (they are excluded from the unit run by the convention plugin).
dependencies {
    api(project(":schema-registry-serde"))
    api(project(":schema-registry-kafkastreams-serde"))
    api(platform(libs.aws.bom))
    api(libs.aws.kinesis)
    api(libs.aws.auth)
    api(libs.kinesis.client) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    api(libs.kinesis.producer)
    api(libs.kafka.clients)
    api(libs.kafka.streams)
    api(libs.localstack.utils)
    api(libs.awaitility)
    api(libs.slf4j.api)
    api(libs.slf4j.simple)
    api(libs.avro)
    api(libs.avro.compiler)
    api(libs.avro.ipc)
    api(libs.protobuf.java)
    api(libs.log4j.api)
    api(libs.log4j.core)
    api(libs.log4j.slf4jImpl)
    api(libs.log4j.compatApi)
    api(libs.junit.jupiter)
    api(libs.hamcrest)
    api(libs.jaxb.api)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Classes generated from serializer-deserializer/src/test/proto, consumed through
    // the test jar just as the Maven `tests` classifier did.
    testImplementation(project(path = ":schema-registry-serde", configuration = "testArtifacts"))
}
