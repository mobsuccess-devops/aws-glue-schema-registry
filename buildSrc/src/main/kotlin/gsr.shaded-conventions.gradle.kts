import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    id("com.gradleup.shadow")
}

// The original pom had maven-shade-plugin replace the main artifact with the uber-jar:
// these modules are dropped as-is onto a Kafka Connect plugin path, where nothing
// resolves transitive dependencies.
tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("")
}

tasks.named<Jar>("jar") {
    enabled = false
}

tasks.named("assemble") {
    dependsOn(tasks.named("shadowJar"))
}

// `from(components["java"])` publishes the artifact of the `jar` task, disabled here.
// Substitute the uber-jar, keeping the dependencies computed for the pom.
publishing.publications.named<MavenPublication>("maven") {
    setArtifacts(
        listOf(
            tasks.named("shadowJar").get(),
            tasks.named("sourcesJar").get(),
        ),
    )
}

// The uber-jar already bundles all of its dependencies: repeating them in the pom
// would have consumers resolve them a second time, with duplicate classes on the
// classpath. This is exactly what maven-shade-plugin's dependency-reduced-pom avoided.
publishing.publications.named<MavenPublication>("maven") {
    pom.withXml {
        val root = asNode()
        val dependencies = root.get("dependencies") as groovy.util.NodeList
        dependencies.forEach { root.remove(it as groovy.util.Node) }
    }
}

// Gradle module metadata would still describe the jar that is no longer produced.
tasks.withType<GenerateModuleMetadata>().configureEach {
    enabled = false
}
