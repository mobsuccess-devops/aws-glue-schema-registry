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

// The inventory covers what the uber-jar actually bundles: the runtime classpath. Both
// filters only tidy up the grouping — LicenseBundleNormalizer maps the many spellings of
// a licence onto a single name, ReduceDuplicateLicensesFilter drops the repeats that
// creates.
licenseReport {
    configurations = arrayOf("runtimeClasspath")
    renderers = arrayOf(InventoryReportRenderer("THIRD-PARTY-LICENSES.txt", project.name))
    filters = arrayOf(LicenseBundleNormalizer(), ReduceDuplicateLicensesFilter())
}

// The renderer stamps its output with the generation date, which would make two builds of
// the same commit produce different jars. Strip that line.
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
    // The uber-jar physically embeds its dependencies, so their attribution notices have
    // to ship with it. The inventory is derived from the resolved graph rather than kept
    // by hand, which is how the inherited THIRD-PARTY-LICENSES.txt ended up describing a
    // dependency set this fork no longer has.
    metaInf {
        from(thirdPartyLicenses)
    }
    // LICENSE.txt and NOTICE.txt come both from the metaInf spec of gsr.java-conventions
    // and from most of the bundled jars. The metaInf spec is the first child spec of Jar,
    // so keeping the first occurrence keeps ours, the ones describing this work.
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
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
