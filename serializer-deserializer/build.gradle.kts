plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.protobuf)
}

coverage {
    minimumInstructionCoverage.set(0.66)
    minimumBranchCoverage.set(0.58)
}

dependencies {
    constraints {
        api(libs.scala.library)
        api(libs.lz4)
    }

    api(project(":schema-registry-common"))
    api(platform(libs.aws.bom))
    api(platform(libs.jackson.bom))
    api(libs.kafka.clients)
    api(libs.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    api(libs.guava)
    api(libs.commons.lang3)
    api(libs.everit.jsonSchema) {
        exclude(group = "commons-collections", module = "commons-collections")
    }
    api(libs.jackson.databind)
    api(libs.protobuf.java)
    api(libs.wire.schema)
    api(libs.kotlin.stdlib)
    api(libs.kotlin.stdlibJdk8)

    implementation(libs.aws.arns)
    implementation(libs.jackson.core)
    implementation(libs.slf4j.api)
    implementation(libs.commons.collections4)
    implementation(libs.protoGoogleCommon)
    implementation(libs.mbknor.jsonSchema) {
        exclude(group = "io.github.classgraph", module = "classgraph")
    }
    implementation(libs.okio)
    implementation(libs.okio.fakeFileSystem)

    runtimeOnly(libs.aws.sts)
    runtimeOnly(libs.lz4)
    runtimeOnly(libs.classgraph)
    runtimeOnly(libs.kotlin.reflect)
    runtimeOnly(libs.kotlin.scriptingCompilerImplEmbeddable)
    runtimeOnly(libs.kotlin.scriptingCompilerEmbeddable)
    runtimeOnly(libs.kotlinx.serializationCore)
    runtimeOnly(libs.wire.compiler) {
        exclude(group = "com.squareup.wire", module = "wire-grpc-client")
        exclude(group = "com.charleskorn.kaml", module = "kaml")
    }

    testImplementation(libs.jackson.annotations)
    testImplementation(libs.jackson.jsr310)
    testImplementation(libs.truth.protoExtension)
}

protobuf {
    protoc {
        artifact = libs.protobuf.protoc.get().toString()
    }
}

val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

val testArtifacts by configurations.consumable("testArtifacts")

artifacts {
    add(testArtifacts.name, testJar)
}
