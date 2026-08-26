import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.filter.ReduceDuplicateLicensesFilter
import com.github.jk1.license.render.InventoryReportRenderer

plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    id("com.gradleup.shadow")
    id("com.github.jk1.dependency-license-report")
}

val slf4jApi =
    extensions
        .getByType<VersionCatalogsExtension>()
        .named("libs")
        .findLibrary("slf4j-api")
        .get()
        .get()

configurations.configureEach {
    exclude(group = "software.amazon.awssdk", module = "apache-client")
    exclude(group = "software.amazon.awssdk", module = "apache5-client")
    exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
    exclude(group = "com.squareup.wire", module = "wire-compiler")
}

licenseReport {
    configurations = arrayOf("runtimeClasspath")
    excludes = arrayOf("${slf4jApi.group}:${slf4jApi.name}")
    renderers = arrayOf(InventoryReportRenderer("THIRD-PARTY-LICENSES.txt", project.name))
    filters = arrayOf(LicenseBundleNormalizer(), ReduceDuplicateLicensesFilter())
}

val thirdPartyLicenses =
    tasks.register("thirdPartyLicenses") {
        group = "documentation"
        description = "Builds the third-party licence inventory bundled in the uber-jar."

        val report =
            layout.buildDirectory.file("reports/dependency-license/THIRD-PARTY-LICENSES.txt")
        val inventory = layout.buildDirectory.file("licenses/THIRD-PARTY-LICENSES.txt")

        dependsOn(tasks.named("generateLicenseReport"))
        inputs.file(report)
        outputs.file(inventory)

        doLast {
            val lines = report.get().asFile.readLines().filterNot { it.startsWith("Generated: ") }
            inventory.get().asFile.writeText(lines.joinToString("\n", postfix = "\n"))
        }
    }

// The original pom had maven-shade-plugin replace the main artifact with the uber-jar:
// these modules are dropped as-is onto a Kafka Connect plugin path, where nothing
// resolves transitive dependencies.
tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("")
    metaInf {
        from(thirdPartyLicenses)
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    dependencies {
        exclude(dependency("${slf4jApi.group}:${slf4jApi.name}:.*"))
    }
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
            tasks.named("javadocJar").get(),
        ),
    )
}

// The uber-jar already bundles all of its dependencies: repeating them in the pom
// would have consumers resolve them a second time, with duplicate classes on the
// classpath. This is exactly what maven-shade-plugin's dependency-reduced-pom avoided.
// slf4j-api is the exception: it is kept out of the jar, so the pom has to ask for it.
publishing.publications.named<MavenPublication>("maven") {
    pom.withXml {
        val root = asNode()
        val dependencies = root.get("dependencies") as groovy.util.NodeList
        dependencies.forEach { root.remove(it as groovy.util.Node) }
        root.appendNode("dependencies").appendNode("dependency").apply {
            appendNode("groupId", slf4jApi.group)
            appendNode("artifactId", slf4jApi.name)
            appendNode("version", slf4jApi.version)
            appendNode("scope", "runtime")
        }
    }
}

// Gradle module metadata would still describe the jar that is no longer produced.
tasks.withType<GenerateModuleMetadata>().configureEach {
    enabled = false
}
