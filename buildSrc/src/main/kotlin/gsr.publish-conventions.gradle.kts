plugins {
    `java-library`
    `maven-publish`
    signing
    id("org.jetbrains.dokka-javadoc")
    id("com.gradleup.nmcp")
}

val javadocJar =
    tasks.register<Jar>("javadocJar") {
        group = "documentation"
        description = "Assembles the Dokka-rendered javadoc jar required by Maven Central."
        archiveClassifier.set("javadoc")
        from(tasks.named("dokkaGeneratePublicationJavadoc"))
    }

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(javadocJar)
            versionMapping {
                allVariants {
                    fromResolutionResult()
                }
            }
            pom {
                name.set(project.name)
                description.set("AWS Glue Schema Registry — ${project.name} (fork Mobsuccess)")
                url.set("https://github.com/mobsuccess-devops/aws-glue-schema-registry")
                inceptionYear.set("2025")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        id.set("mobsuccess-devops")
                        name.set("Mobsuccess")
                        url.set("https://github.com/mobsuccess-devops")
                        organization.set("Mobsuccess")
                        organizationUrl.set("https://www.mobsuccess.com")
                    }
                }
                scm {
                    url.set("https://github.com/mobsuccess-devops/aws-glue-schema-registry")
                    connection.set("scm:git:git://github.com/mobsuccess-devops/aws-glue-schema-registry.git")
                    developerConnection.set(
                        "scm:git:ssh://git@github.com/mobsuccess-devops/aws-glue-schema-registry.git",
                    )
                }
                issueManagement {
                    system.set("GitHub Issues")
                    url.set("https://github.com/mobsuccess-devops/aws-glue-schema-registry/issues")
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry")
            credentials {
                username = "_"
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key") as String?
            }
        }
    }
}

val signingKey = providers.gradleProperty("signingInMemoryKey").orNull?.takeIf { it.isNotBlank() }
val signingKeyPassword = providers.gradleProperty("signingInMemoryKeyPassword").orNull?.takeIf { it.isNotBlank() }

signing {
    isRequired = signingKey != null
    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingKeyPassword)
        sign(publishing.publications["maven"])
    }
}
