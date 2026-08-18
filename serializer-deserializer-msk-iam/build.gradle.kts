plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    alias(libs.plugins.shadow)
}

dependencies {
    api(project(":schema-registry-serde"))
    api(libs.aws.mskIamAuth)
}

// Le pom d'origine remplaçait l'artefact principal par l'uber-jar du maven-shade-plugin.
tasks.shadowJar {
    archiveClassifier.set("")
}

tasks.jar {
    enabled = false
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
}
