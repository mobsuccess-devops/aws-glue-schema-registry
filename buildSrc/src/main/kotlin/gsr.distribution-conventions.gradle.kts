import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.filter.ReduceDuplicateLicensesFilter
import com.github.jk1.license.render.InventoryReportRenderer

plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    id("com.github.jk1.dependency-license-report")
}

licenseReport {
    configurations = arrayOf("runtimeClasspath")
    renderers = arrayOf(InventoryReportRenderer("THIRD-PARTY-LICENSES.txt", project.name))
    filters = arrayOf(LicenseBundleNormalizer(), ReduceDuplicateLicensesFilter())
}

val thirdPartyLicenses =
    tasks.register("thirdPartyLicenses") {
        group = "documentation"
        description = "Builds the third-party licence inventory shipped in the plugin distribution."

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

val pluginDistribution =
    tasks.register<Zip>("pluginDistribution") {
        group = LifecycleBasePlugin.BUILD_GROUP
        description = "Packages the jar and its runtime classpath as a Kafka Connect plugin directory."

        archiveClassifier.set("plugin")
        val root = "${project.name}-${project.version}"

        into("$root/lib") {
            from(tasks.named("jar"))
            from(configurations.named("runtimeClasspath"))
        }
        into(root) {
            from(rootProject.layout.projectDirectory.file("LICENSE.txt"))
            from(rootProject.layout.projectDirectory.file("NOTICE.txt"))
            from(thirdPartyLicenses)
        }
    }

tasks.named("assemble") {
    dependsOn(pluginDistribution)
}
