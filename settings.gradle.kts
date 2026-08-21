rootProject.name = "aws-glue-schema-registry"

// Gradle project names reuse the original Maven artifactIds so that the published
// coordinates stay unchanged, while the directories keep the source repository layout.
//
// The Maven `build-tools` module is not carried over: it only held the Checkstyle
// configuration of the Maven build, replaced here by ktlint (see .editorconfig).
private val modules = mapOf(
    "schema-registry-common" to "common",
    "schema-registry-serde" to "serializer-deserializer",
    "schema-registry-serde-kotlin" to "serde-kotlin",
    "schema-registry-serde-msk-iam" to "serializer-deserializer-msk-iam",
    "schema-registry-kafkastreams-serde" to "kafkastreams-serde",
    "schema-registry-kafkaconnect-converter" to "avro-kafkaconnect-converter",
    "schema-registry-flink-serde" to "avro-flink-serde",
    "jsonschema-kafkaconnect-converter" to "jsonschema-kafkaconnect-converter",
    "protobuf-kafkaconnect-converter" to "protobuf-kafkaconnect-converter",
    "schema-registry-examples" to "examples",
    "schema-registry-integration-tests" to "integration-tests",
)

modules.forEach { (name, dir) ->
    include(name)
    project(":$name").projectDir = file(dir)
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
    }
}
