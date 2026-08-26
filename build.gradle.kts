// The root build carries no code: shared configuration lives in the `gsr.*`
// convention plugins (buildSrc), applied module by module.
plugins {
    alias(libs.plugins.binaryCompatibilityValidator)
    id("com.gradleup.nmcp.aggregation")
}

apiValidation {
    ignoredProjects += listOf("schema-registry-examples", "schema-registry-integration-tests")
}

nmcpAggregation {
    centralPortal {
        username.set(providers.gradleProperty("mavenCentralUsername").filter { it.isNotBlank() })
        password.set(providers.gradleProperty("mavenCentralPassword").filter { it.isNotBlank() })
        publishingType.set(
            providers.gradleProperty("centralPublishingType").orElse("USER_MANAGED"),
        )
    }
}

dependencies {
    nmcpAggregation(project(":schema-registry-common"))
    nmcpAggregation(project(":schema-registry-serde"))
    nmcpAggregation(project(":schema-registry-serde-kotlin"))
    nmcpAggregation(project(":schema-registry-serde-msk-iam"))
    nmcpAggregation(project(":schema-registry-kafkastreams-serde"))
    nmcpAggregation(project(":schema-registry-kafkaconnect-converter"))
    nmcpAggregation(project(":schema-registry-flink-serde"))
    nmcpAggregation(project(":jsonschema-kafkaconnect-converter"))
    nmcpAggregation(project(":protobuf-kafkaconnect-converter"))
}

tasks.register("printModules") {
    group = "help"
    description = "Lists the published modules and their artifactId."
    val modules = subprojects.associate { it.path to it.name }
    doLast { modules.forEach { (path, name) -> println("$path -> $name") } }
}
