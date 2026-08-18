import org.gradle.accessors.dm.LibrariesForLibs

plugins {
    `java-library`
    jacoco
}

val libs = the<LibrariesForLibs>()

group = "com.mobsuccess.glue"
version = System.getenv("PACKAGE_VERSION") ?: "0.0.0-LOCAL"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
    withSourcesJar()
}

configurations.configureEach {
    // Le pom d'origine excluait org.lz4:lz4-java de chaque artefact Kafka au profit du
    // fork at.yawk.lz4:lz4-java. Les deux déclarent la même capability `org.lz4:lz4-java` :
    // sans cette exclusion, Gradle refuse de trancher entre les deux.
    exclude(group = "org.lz4", module = "lz4-java")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
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
    // Reproduit l'exclusion surefire du pom parent : les *IntegrationTest exigent
    // des ressources AWS réelles et ne font pas partie du run unitaire.
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
