# Installation

How to depend on this fork: the Maven Central coordinates, the versions each release is built
against, and the move from the AWS artifact. The list of artifacts is in the
[README](../README.md#packages).

Releases are published to **Maven Central** under the `com.mobsuccess` group. Nothing to
declare beyond the coordinate, and no authentication — `mavenCentral()` is already in every
JVM build.

Snapshots are a separate channel: each push to `master` publishes `<next-version>-SNAPSHOT`
to **GitHub Packages**, which does require a token. See
[Snapshots](#snapshots-github-packages) below.

## 1. Add the dependency

**Gradle (Kotlin DSL)**:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("com.mobsuccess:schema-registry-serde:<version>")
}
```

**Gradle (Groovy DSL)**:

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'com.mobsuccess:schema-registry-serde:<version>'
}
```

**Maven**:

```xml
<dependency>
  <groupId>com.mobsuccess</groupId>
  <artifactId>schema-registry-serde</artifactId>
  <version><!-- version --></version>
</dependency>
```

Most applications only need `schema-registry-serde`. The other artifacts are listed in the
[README](../README.md#packages).

## 2. Pick a version

The latest release is on the [releases page](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases/latest),
and every published version is listed on
[Maven Central](https://central.sonatype.com/namespace/com.mobsuccess). Releases are what you
want in production.

Every release is signed with the project's PGP key and carries a sources jar and a javadoc
jar, so an IDE resolves documentation and sources without extra configuration.

## Snapshots (GitHub Packages)

Snapshots are not published to Maven Central. Each push to `master` publishes
`<next-version>-SNAPSHOT` to GitHub Packages, which requires a **personal access token
(classic)** even to read a public repository —
[fine-grained tokens are not supported](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-gradle-registry).

Create one at [**Settings → Developer settings → Personal access tokens (classic)**](https://github.com/settings/tokens/new?scopes=read:packages&description=aws-glue-schema-registry),
with the single scope `read:packages`, and export it:

```bash
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

The token is what authenticates; the username only has to be a real GitHub login. Keep the
token out of the build files — pass it through the environment or `~/.gradle/gradle.properties`.

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
        mavenContent { snapshotsOnly() }
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
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
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

Releases stay on Maven Central and are also mirrored to GitHub Packages, so an existing
GitHub Packages setup keeps resolving them. New consumers should take releases from Central.

## Compatibility

The versions the artifacts are built and tested against. Everything except the JVM row comes
from `gradle/libs.versions.toml`, which is the single source of truth for the build.

| Component           | Version            | Notes                                                                                                                                                                             |
| ------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| JVM                 | 17 or later        | Bytecode target is 17, so a JVM 8 or 11 runtime cannot load these artifacts.                                                                                                      |
| Apache Kafka        | 3.9.x              | `kafka-clients`, `kafka-streams`, `connect-api`, `connect-json`. Shaded into the uber-jars: a consumer cannot override that copy.                                                 |
| Apache Avro         | 1.11.4             |                                                                                                                                                                                   |
| Protocol Buffers    | 4.36.0             | `protobuf-java`; syntax 2 and 3. On `master`, shipping in the next major — releases up to 2.1.0 resolve 3.25.5. A consumer on protobuf 3 has to move: 4 is not binary compatible. |
| AWS SDK for Java v2 | 2.53.1             | Imported as a BOM, so the whole SDK moves together.                                                                                                                               |
| MSK IAM auth        | 2.3.7              | `schema-registry-serde-msk-iam` only.                                                                                                                                             |
| SLF4J               | 2.0.x              | `slf4j-api`; the one dependency the uber-jars do not bundle. See below.                                                                                                           |
| Apache Flink        | 1.12.2, Scala 2.11 | **Not recommended** — see below.                                                                                                                                                  |

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

## GraalVM native image

The serde jars carry their own reachability metadata, so a native consumer needs no extra
configuration for this library. What it does still have to declare, and how far the support
has been verified, are in [native-image.md](native-image.md).

## Migrating from the AWS artifact

Coming from `software.amazon.glue`, the swap is a coordinate change: both are on Maven
Central, and the artifactIds, the package names and the class names are all unchanged.

```diff
- implementation("software.amazon.glue:schema-registry-serde:1.1.x")
+ implementation("com.mobsuccess:schema-registry-serde:<version>")
```

Two things to check on the way:

1. **The JVM.** Upstream targeted 8, this fork targets 17.
2. **Two behaviour deltas**, both deliberate:
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

## Migrating from GitHub Packages

Consumers wired to GitHub Packages before Maven Central existed can drop the repository block
and the token entirely, as long as they only resolve releases:

```diff
 repositories {
     mavenCentral()
-    maven {
-        name = "GitHubPackages"
-        url = uri("https://maven.pkg.github.com/mobsuccess-devops/aws-glue-schema-registry")
-        credentials { /* … */ }
-    }
 }
```

Keep the repository block only if you consume `-SNAPSHOT` versions, and restrict it to
snapshots with `mavenContent { snapshotsOnly() }` so releases resolve from Central. The
coordinates and versions are identical on both channels.

## Troubleshooting

| Symptom                                                            | Cause                                                                                                                                       |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `Could not find com.mobsuccess:...`                                | `mavenCentral()` is missing from the repositories, or the version does not exist. A `-SNAPSHOT` version resolves from GitHub Packages only. |
| `401 Unauthorized` / `403 Forbidden` on GitHub Packages            | Snapshot channel only: no token, an expired token, a fine-grained token, or a token without `read:packages`.                                |
| `Unsupported class file major version` / `UnsupportedClassVersion` | The runtime is older than JVM 17.                                                                                                           |
