import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("gsr.java-conventions")
    id("gsr.publish-conventions")
    id("com.gradleup.shadow")
}

// Le pom d'origine faisait remplacer l'artefact principal par l'uber-jar du
// maven-shade-plugin : ces modules sont déposés tels quels sur un plugin path
// Kafka Connect, où rien ne résout les dépendances transitives.
tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("")
}

tasks.named<Jar>("jar") {
    enabled = false
}

tasks.named("assemble") {
    dependsOn(tasks.named("shadowJar"))
}

// `from(components["java"])` publie l'artefact de la tâche `jar`, ici désactivée.
// On substitue l'uber-jar, en gardant les dépendances calculées pour le pom.
publishing.publications.named<MavenPublication>("maven") {
    setArtifacts(
        listOf(
            tasks.named("shadowJar").get(),
            tasks.named("sourcesJar").get(),
        ),
    )
}

// L'uber-jar embarque déjà toutes ses dépendances : les répéter dans le pom les
// ferait résoudre une seconde fois chez le consommateur, avec des classes en
// double. C'est exactement ce qu'évitait le dependency-reduced-pom du
// maven-shade-plugin.
publishing.publications.named<MavenPublication>("maven") {
    pom.withXml {
        val root = asNode()
        val dependencies = root.get("dependencies") as groovy.util.NodeList
        dependencies.forEach { root.remove(it as groovy.util.Node) }
    }
}

// Les métadonnées de module Gradle décriraient encore le jar non produit.
tasks.withType<GenerateModuleMetadata>().configureEach {
    enabled = false
}
