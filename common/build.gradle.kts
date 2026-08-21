plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(platform(libs.aws.bom))
    api(platform(libs.jackson.bom))
    api(libs.aws.glue)
    api(libs.jackson.databind)
    implementation(libs.aws.urlConnectionClient)
    implementation(libs.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    implementation(libs.slf4j.api)
    implementation(libs.guava)
    implementation(libs.commons.lang3)
    runtimeOnly(libs.aws.jsonProtocol)
}

// Remplace le templating-maven-plugin : injecte la version du projet dans
// MavenPackaging.VERSION, lue par l'intercepteur de User-Agent des appels Glue.
val generateVersionSource by tasks.registering(Copy::class) {
    inputs.property("version", project.version)
    from("src/main/java-templates")
    into(layout.buildDirectory.dir("generated/sources/version/java/main"))
    expand("project" to mapOf("version" to project.version.toString()))
}

sourceSets.main {
    java.srcDir(generateVersionSource)
}
