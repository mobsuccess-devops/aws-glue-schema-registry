plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
}

dependencies {
    api(platform(libs.aws.bom))
    api(libs.aws.glue)
    api(libs.aws.jsonProtocol)
    api(libs.aws.urlConnectionClient)
    api(libs.avro) {
        exclude(group = "org.apache.commons", module = "commons-compress")
    }
    api(libs.slf4j.api)
    api(libs.guava)
    api(libs.commons.lang3)
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
