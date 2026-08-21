// The root build carries no code: shared configuration lives in the `gsr.*`
// convention plugins (buildSrc), applied module by module.
plugins {
    alias(libs.plugins.binaryCompatibilityValidator)
}

apiValidation {
    ignoredProjects += listOf("schema-registry-examples", "schema-registry-integration-tests")
}

tasks.register("printModules") {
    group = "help"
    description = "Lists the published modules and their artifactId."
    val modules = subprojects.associate { it.path to it.name }
    doLast { modules.forEach { (path, name) -> println("$path -> $name") } }
}
