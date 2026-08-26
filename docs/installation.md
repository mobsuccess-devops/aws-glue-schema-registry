# Installation

How to depend on this fork: the GitHub Packages setup, the versions each release is built
against, and the move from the AWS artifact on Maven Central. The list of artifacts is in the
[README](../README.md#packages).

Artifacts are published to **GitHub Packages**, not to Maven Central.

> **GitHub Packages requires a token even to read a public repository.** This is a GitHub
> limitation, not a choice of this project: anonymous reads of the Maven registry are not
> supported for any repository, public or private. Publishing to Maven Central, which would
> remove that step, is planned but not done.

## 1. Create a token

GitHub Packages' Maven and Gradle registries only accept a **personal access token
(classic)** — [fine-grained tokens are not
supported](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry).

Create one at [**Settings → Developer settings → Personal access tokens (classic)**](https://github.com/settings/tokens/new?scopes=read:packages&description=aws-glue-schema-registry),
with the single scope `read:packages`, and export it:

```bash
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

The token is what authenticates; the username only has to be a real GitHub login. Keep the
token out of the build files — pass it through the environment or `~/.gradle/gradle.properties`.

## 2. Declare the repository

**Gradle (Kotlin DSL)** — in `settings.gradle.kts` under `dependencyResolutionManagement`,
or in `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry")
        credentials {
            username = providers.gradleProperty("gpr.user").orNull
                ?: System.getenv("GITHUB_ACTOR")
            password = providers.gradleProperty("gpr.token").orNull
                ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("com.mobsuccess:schema-registry-serde:<version>")
}
```

**Gradle (Groovy DSL)**:

```groovy
repositories {
    mavenCentral()
    maven {
        name = 'GitHubPackages'
        url = 'https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry'
        credentials {
            username = findProperty('gpr.user') ?: System.getenv('GITHUB_ACTOR')
            password = findProperty('gpr.token') ?: System.getenv('GITHUB_TOKEN')
        }
    }
}
```

**Maven** — the repository goes in `pom.xml`, the credentials in `~/.m2/settings.xml`, keyed
by the same `<id>`:

```xml
<!-- pom.xml -->
<repositories>
  <repository>
    <id>github-mobsuccess</id>
    <url>https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>com.mobsuccess</groupId>
    <artifactId>schema-registry-serde</artifactId>
    <version><!-- version --></version>
  </dependency>
</dependencies>
```

```xml
<!-- ~/.m2/settings.xml -->
<servers>
  <server>
    <id>github-mobsuccess</id>
    <username>your-github-username</username>
    <password>${env.GITHUB_TOKEN}</password>
  </server>
</servers>
```

**In CI**, store the classic token as a secret and export it as `GITHUB_TOKEN`. The
automatic `GITHUB_TOKEN` of a workflow in _another_ repository does not carry read access to
this one's packages unless that access has been granted explicitly, so a workflow secret is
the reliable path.

## 3. Pick a version

The latest release is on the [releases page](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases/latest);
every published version is listed under the repository's
[Packages](https://github.com/orgs/mobsuccess-devops/packages?repo_name=aws-glue-schema-registry).
Each push to `master` also publishes a `<next-version>-SNAPSHOT`; releases are what you want
in production.

## Compatibility

The versions the artifacts are built and tested against. Everything except the JVM row comes
from `gradle/libs.versions.toml`, which is the single source of truth for the build.

| Component           | Version            | Notes                                                                                                                             |
| ------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| JVM                 | 17 or later        | Bytecode target is 17, so a JVM 8 or 11 runtime cannot load these artifacts.                                                      |
| Apache Kafka        | 3.9.x              | `kafka-clients`, `kafka-streams`, `connect-api`, `connect-json`. Shaded into the uber-jars: a consumer cannot override that copy. |
| Apache Avro         | 1.11.4             |                                                                                                                                   |
| Protocol Buffers    | 4.36.0             | `protobuf-java`; syntax 2 and 3. A consumer on protobuf 3 has to move: 4 is not binary compatible.                                |
| AWS SDK for Java v2 | 2.53.1             | Imported as a BOM, so the whole SDK moves together.                                                                               |
| MSK IAM auth        | 2.3.7              | `schema-registry-serde-msk-iam` only.                                                                                             |
| SLF4J               | 2.0.x              | `slf4j-api`; the one dependency the uber-jars do not bundle. See below.                                                           |
| Apache Flink        | 1.12.2, Scala 2.11 | **Not recommended** — see below.                                                                                                  |

The Flink connector is carried over from upstream unchanged and is pinned to Flink 1.12.2 with
`flink-streaming-java_2.11`, a Scala 2.11 coordinate that Flink stopped publishing after 1.14.
It is kept so the fork stays behaviour-identical to its source, not because it is a reasonable
dependency to take today. New Flink work should use the Glue Schema Registry formats that ship
with [Apache Flink itself](https://github.com/apache/flink/tree/master/flink-formats).

The uber-jars — the Connect converters and `schema-registry-serde-msk-iam` — bundle every
dependency they resolve except `slf4j-api`, which their pom declares instead, so that the
logging stack in effect is the application's own. A Kafka Connect worker already provides
both the API and a binding; a standalone application needs an slf4j 2.x binding of its own
— `logback-classic` 1.3 or later, `log4j-slf4j2-impl` — or slf4j falls back to its no-op
logger.

## Migrating from the AWS artifact

Coming from `software.amazon.glue` on Maven Central, the swap is a coordinate change: the
artifactIds, the package names and the class names are all unchanged.

```diff
- implementation("software.amazon.glue:schema-registry-serde:1.1.x")
+ implementation("com.mobsuccess:schema-registry-serde:<version>")
```

Three things to check on the way:

1. **The repository.** GitHub Packages requires authentication even to read a public
   repository, and its Maven registry only accepts a _classic_ personal access token. Add
   the repository block and the token from [1. Create a token](#1-create-a-token) — this is the one
   step that has no equivalent when consuming from Maven Central.
2. **The JVM.** Upstream targeted 8, this fork targets 17.
3. **Two behaviour deltas**, both deliberate:
   - A `@NonNull` violation raises a `NullPointerException` rather than the
     `IllegalArgumentException` upstream's `lombok.config` produced. A null argument is still
     rejected, at the same point; only the exception type differs. See
     [portage.md](portage.md).
   - Since **2.0.0**, the JSON deserializer no longer resolves a schema's `className` into a
     POJO by default and returns `JsonDataWithSchema` instead. Restoring the old behaviour
     takes both `jsonClassNameResolutionEnabled=true` and an explicit
     `jsonClassNameAllowlist`. It is inherited from upstream rather than introduced here —
     see [Deserializing JSON into a Java POJO](usage.md#deserializing-json-into-a-java-pojo-classname-resolution)
     and [upstream-history.md](upstream-history.md).

## Troubleshooting

| Symptom                                                            | Cause                                                                                         |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `401 Unauthorized`                                                 | No token, an expired token, or a fine-grained token — the Maven registry needs a classic one. |
| `403 Forbidden`                                                    | The token exists but lacks the `read:packages` scope.                                         |
| `Could not find com.mobsuccess:...`                                | The repository block is missing, or the version does not exist.                               |
| `Unsupported class file major version` / `UnsupportedClassVersion` | The runtime is older than JVM 17.                                                             |
