import org.gradle.accessors.dm.LibrariesForLibs
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import java.math.BigDecimal

plugins {
    `java-library`
    jacoco
    kotlin("jvm")
}

val libs = the<LibrariesForLibs>()

// The organization group, as in geojson-jackson. Deliberately different from
// `software.amazon.glue`: an artifact of this fork must never be able to substitute
// itself for the Maven Central one in a consumer's dependency graph.
group = "com.mobsuccess"
version = System.getenv("PACKAGE_VERSION") ?: "0.0.0-LOCAL"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
    withSourcesJar()
}

configurations.configureEach {
    // The original pom excluded org.lz4:lz4-java from every Kafka artifact in favour of
    // the at.yawk.lz4:lz4-java fork. Both declare the same `org.lz4:lz4-java` capability:
    // without this exclusion, Gradle refuses to arbitrate between them.
    exclude(group = "org.lz4", module = "lz4-java")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
}

tasks.withType<Jar>().configureEach {
    metaInf {
        from(rootProject.layout.projectDirectory.file("LICENSE.txt"))
        from(rootProject.layout.projectDirectory.file("NOTICE.txt"))
    }
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

// Kotlin compiles first, then javac sees its classes. The only Java left is the Avro
// classes generated into the test trees, which reference nothing Kotlin.
kotlin {
    explicitApiWarning()
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiterApi)
    testImplementation(libs.junit.jupiterParams)
    testImplementation(libs.junit.platformCommons)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junitJupiter)
    testImplementation(libs.mockito.kotlin)
    testRuntimeOnly(libs.junit.jupiterEngine)
    // Gradle 9 requires the JUnit platform launcher on the test classpath.
    testRuntimeOnly(libs.junit.platformLauncher)
}

val testJavaVersion =
    providers.gradleProperty("testJavaVersion").orNull?.let { requested ->
        val version =
            requested.toIntOrNull()?.takeIf { it > 0 }
                ?: throw GradleException(
                    "testJavaVersion must be a positive integer, not '$requested'. " +
                        "Pass a major version, for instance -PtestJavaVersion=21.",
                )
        JavaLanguageVersion.of(version)
    } ?: JavaLanguageVersion.of(17)

val testJavaLauncher =
    extensions.getByType<JavaToolchainService>().launcherFor {
        languageVersion.set(testJavaVersion)
    }

val testForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)

tasks.test {
    useJUnitPlatform()
    javaLauncher.set(testJavaLauncher)
    maxParallelForks = testForks
    // Mirrors the surefire exclusion of the parent pom: *IntegrationTest classes need
    // real AWS resources and are not part of the unit run.
    filter {
        excludeTestsMatching("*IntegrationTest")
        isFailOnNoMatchingTests = false
    }
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// The counterpart of that exclusion: the same test source set, filtered the other way.
// Kept out of `check` on purpose — these tests need a Kafka broker, LocalStack and a Glue
// endpoint, so they are driven by the integration workflow rather than by every build.
val integrationTest by tasks.registering(Test::class) {
    description = "Runs the *IntegrationTest classes that the test task leaves out."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    val testSourceSet = project.the<SourceSetContainer>()["test"]
    testClassesDirs = testSourceSet.output.classesDirs
    classpath = testSourceSet.runtimeClasspath
    useJUnitPlatform()
    // KPL and KCL write megabytes to stdout, and Gradle embeds that in the JUnit XML as one
    // enormous CDATA section, which the results publisher refuses to parse - reporting a
    // failure on a suite that passed. The HTML report keeps the output for diagnosis.
    reports.junitXml.includeSystemOutLog = false
    filter {
        includeTestsMatching("*IntegrationTest")
        isFailOnNoMatchingTests = false
    }
    shouldRunAfter(tasks.test)
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}

val coverage = extensions.create<GsrCoverageExtension>("coverage")
coverage.enabled.convention(true)
coverage.minimumInstructionCoverage.convention(0.60)
coverage.minimumBranchCoverage.convention(0.50)

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    onlyIf { coverage.enabled.get() }
    violationRules {
        rule {
            limit {
                counter = "INSTRUCTION"
                value = "COVEREDRATIO"
                minimum = BigDecimal.valueOf(coverage.minimumInstructionCoverage.get())
            }
            limit {
                counter = "BRANCH"
                value = "COVEREDRATIO"
                minimum = BigDecimal.valueOf(coverage.minimumBranchCoverage.get())
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}
