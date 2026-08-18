plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    // Expose les accesseurs typés du catalogue (`libs.…`) aux plugins de
    // convention précompilés, qui n'y ont pas accès autrement.
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))

    // Nécessaire pour appliquer le plugin shadow depuis gsr.shaded-conventions.
    implementation(libs.shadow.plugin)

    // Nécessaire pour appliquer le plugin Kotlin depuis gsr.java-conventions.
    implementation(libs.kotlin.gradlePlugin)
}
