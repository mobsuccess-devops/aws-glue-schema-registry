plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    // Exposes the catalog's type-safe accessors (`libs.…`) to the precompiled
    // convention plugins, which cannot reach them otherwise.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))

    implementation(platform(libs.jackson.bom))

    implementation(libs.licenseReport.plugin)

    implementation(libs.dokka.plugin)

    implementation(libs.nmcp.plugin)

    // Required to apply the Kotlin plugin from gsr.java-conventions.
    implementation(libs.kotlin.gradlePlugin)

    implementation(libs.avro.gradlePlugin)

    constraints {
        implementation(libs.avro)
        implementation(libs.avro.compiler)
        implementation(libs.commons.lang3)
        implementation(libs.commons.compress)
    }
}
