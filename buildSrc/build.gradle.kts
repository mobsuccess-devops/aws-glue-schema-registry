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

    // Required to apply the shadow plugin from gsr.shaded-conventions.
    implementation(libs.shadow.plugin)

    implementation(libs.licenseReport.plugin)

    implementation(libs.dokka.plugin)

    implementation(libs.nmcp.plugin)

    // Required to apply the Kotlin plugin from gsr.java-conventions.
    implementation(libs.kotlin.gradlePlugin)
}
