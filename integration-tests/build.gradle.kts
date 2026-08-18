plugins {
    id("gsr.java-conventions")
}

// Module de tests d'intégration : rien à publier, et ses tests exigent des
// ressources AWS réelles (ils sont exclus du run unitaire par la convention).
dependencies {
    api(project(":schema-registry-serde"))
    api(project(":schema-registry-kafkastreams-serde"))
    api(platform(libs.aws.bom))
    api(libs.aws.kinesis)
    api(libs.aws.auth)
    api(libs.kinesis.client) {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    api(libs.kinesis.producer) {
        exclude(group = "software.amazon.ion", module = "ion-java")
    }
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
    api(libs.hamcrest.all)
    api(libs.jaxb.api)

    // Classes générées depuis serializer-deserializer/src/test/proto, consommées
    // via le jar de tests comme le faisait le classifier Maven `tests`.
    testImplementation(project(path = ":schema-registry-serde", configuration = "testArtifacts"))
}
