plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.protobuf)
}

dependencies {
    api(project(":schema-registry-common"))
    api(platform(libs.aws.bom))
    api(libs.aws.sts)
    api(libs.aws.arns)
    api(libs.aws.v1.sts)
    api(libs.kafka.clients)
    api(libs.lz4)
    api(libs.mbknor.jsonSchema) {
        exclude(group = "io.github.classgraph", module = "classgraph")
    }
    api(libs.classgraph)
    api(libs.everit.jsonSchema) {
        exclude(group = "commons-collections", module = "commons-collections")
    }
    api(libs.commons.collections4)
    api(libs.protobuf.java)
    api(libs.kotlin.stdlib)
    api(libs.kotlin.stdlibJdk8)
    api(libs.kotlin.reflect)
    api(libs.kotlin.scriptingCompilerImplEmbeddable)
    api(libs.kotlin.scriptingCompilerEmbeddable)
    api(libs.okio)
    api(libs.okio.fakeFileSystem)
    api(libs.kotlinx.serializationCore)
    api(libs.wire.schema)
    api(libs.wire.compiler) {
        exclude(group = "com.squareup.wire", module = "wire-grpc-client")
        exclude(group = "com.charleskorn.kaml", module = "kaml")
    }
    api(libs.protoGoogleCommon)

    testImplementation(libs.truth.protoExtension)
}

protobuf {
    protoc {
        artifact = libs.protobuf.protoc.get().toString()
    }
}

// The pom published a test jar (classifier `tests`): the classes generated from
// src/test/proto are reused there by the integration-tests module.
val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

val testArtifacts by configurations.consumable("testArtifacts")

artifacts {
    add(testArtifacts.name, testJar)
}

publishing.publications.named<MavenPublication>("maven") {
    artifact(testJar)
}
