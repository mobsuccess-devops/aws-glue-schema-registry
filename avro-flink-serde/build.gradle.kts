plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    // Le pom d'origine pointait vers schema-registry-serde publié sur Maven Central
    // (2.0.0 en compile, 1.0.2 en test) plutôt que vers le module local ; on rétablit
    // la dépendance interne, seule cohérente dans un build multi-modules.
    api(project(":schema-registry-serde"))
    api(libs.flink.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    compileOnly(libs.flink.streamingJava) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }

    testImplementation(libs.flink.streamingJava) {
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "org.lz4", module = "lz4-java")
    }
    testImplementation(libs.junit4)
    testImplementation(libs.hamcrest)
}
