plugins {
    `java-library`
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set(project.name)
                description.set("AWS Glue Schema Registry — ${project.name} (fork Mobsuccess)")
                url.set("https://github.com/mobsuccess-devops/aws-glue-schema-registry")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                scm {
                    url.set("https://github.com/mobsuccess-devops/aws-glue-schema-registry")
                    connection.set("scm:git:git://github.com/mobsuccess-devops/aws-glue-schema-registry.git")
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
