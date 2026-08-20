import org.gradle.accessors.dm.LibrariesForLibs
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `java-library`
    jacoco
    kotlin("jvm")
    // Without this plugin, Kotlin cannot see the accessors Lombok generates on the
    // Java classes still to be converted, and resolves to their private fields instead.
    kotlin("plugin.lombok")
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

// Apache License 2.0 §4 requires a copy of the licence and the contents of the NOTICE
// file to travel with every redistributed copy of the work. The Maven build packaged
// neither, so a consumer receiving only the jar from GitHub Packages got no licence and
// no attribution. ShadowJar extends Jar and is covered by this too.
tasks.withType<Jar>().configureEach {
    metaInf {
        from(rootProject.layout.projectDirectory.file("LICENSE.txt"))
        from(rootProject.layout.projectDirectory.file("NOTICE.txt"))
    }
}

// Java and Kotlin coexist for the duration of the conversion: Kotlin compiles first,
// then javac sees its classes. The sources still in Java therefore validate the code
// already converted, without the tests having been touched.
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

dependencies {
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiterApi)
    testImplementation(libs.junit.jupiterParams)
    testImplementation(libs.junit.platformCommons)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junitJupiter)
    testRuntimeOnly(libs.junit.jupiterEngine)
    // Gradle 9 exige le launcher de la plateforme JUnit sur le classpath de test.
    testRuntimeOnly(libs.junit.platformLauncher)
}

tasks.test {
    useJUnitPlatform()
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

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}
