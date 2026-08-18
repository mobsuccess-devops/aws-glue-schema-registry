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
}
