rootProject.name = "aws-glue-schema-registry"

// Les noms de projet Gradle reprennent les artifactId Maven d'origine pour que les
// coordonnées publiées restent inchangées, tandis que les répertoires gardent
// l'arborescence du repo source.
//
// Le module Maven `build-tools` n'est pas repris : il ne portait que la configuration
// Checkstyle du build Maven, remplacée ici par ktlint (cf. .editorconfig).
private val modules = mapOf(
    "schema-registry-common" to "common",
    "schema-registry-serde" to "serializer-deserializer",
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
    repositories {
        mavenCentral()
    }
}
