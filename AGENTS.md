# aws-glue-schema-registry

Mobsuccess fork of [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry),
reduced to the Java part, ported from Maven to Gradle, and converted to Kotlin —
everything but `integration-tests`, which is still Java.

## The golden rule

The repository must stay **behaviour-identical to the source at the Java level**. The
inherited test suite is the only guard rail: **it must stay green, and it must never
shrink**. A conversion step that drops a test or turns one red is not finished, however
good the produced code looks.

The first commit is the `awslabs` source verbatim (`eed1506`), so every deviation reads
with `git diff eed1506 -- <path>`.

## Commands

```bash
./gradlew clean build   # compile + full test suite + jars
./gradlew test          # tests only
./gradlew apiCheck      # public ABI against the committed dumps; runs as part of `check`
./gradlew apiDump       # accept a deliberate ABI change, then commit the .api diff
```

`*IntegrationTest` classes are excluded from `test` and `check`. They run through the
`integrationTest` task and need external services — see [docs/build.md](docs/build.md).

## Modules

Eleven of them. Ten mirror the directories of the source repository and reuse the Maven
artifactIds as their Gradle project names; `serde-kotlin` is the fork's own addition. The
graph is linear — `common` → `serializer-deserializer` → all the others — and that is the
order to work in.

| Directory                           | Artifact                                 | Role                                     |
| ----------------------------------- | ---------------------------------------- | ---------------------------------------- |
| `common`                            | `schema-registry-common`                 | Glue client, cache, exceptions           |
| `serializer-deserializer`           | `schema-registry-serde`                  | SerDe core (Avro, JSON Schema, Protobuf) |
| `serde-kotlin`                      | `schema-registry-serde-kotlin`           | Kotlin DSL and typed `Serde<T>`          |
| `serializer-deserializer-msk-iam`   | `schema-registry-serde-msk-iam`          | uber-jar SerDe + MSK IAM auth            |
| `kafkastreams-serde`                | `schema-registry-kafkastreams-serde`     | Kafka Streams integration                |
| `avro-kafkaconnect-converter`       | `schema-registry-kafkaconnect-converter` | Connect Avro converter                   |
| `avro-flink-serde`                  | `schema-registry-flink-serde`            | Flink (de)serialization schemas          |
| `jsonschema-kafkaconnect-converter` | `jsonschema-kafkaconnect-converter`      | Connect JSON Schema converter            |
| `protobuf-kafkaconnect-converter`   | `protobuf-kafkaconnect-converter`        | Connect Protobuf converter               |
| `examples`                          | `schema-registry-examples`               | integration examples                     |
| `integration-tests`                 | `schema-registry-integration-tests`      | tests requiring real AWS resources       |

## Rules for every change

- **Writing or editing Kotlin? [docs/kotlin-interop.md](docs/kotlin-interop.md) lists
  where Kotlin and Java do not line up** — the API-shape rules, and the changes that keep
  the build green while moving the output. Each one cost a red test at least once.
- **Run the whole build before committing.** A module's own tests do not cover the modules
  that consume it.
- **Never hard-code a version** in a `build.gradle.kts` — everything goes through
  `gradle/libs.versions.toml`. Shared configuration lives in
  `buildSrc/src/main/kotlin/gsr.*.gradle.kts`; the root build has no `subprojects {}`.
- **A red `apiCheck` is not a formality.** Read the diff it prints, decide whether the
  signature change is deliberate, then run `apiDump`.
- **Everything that lands on GitHub is in English**: commit messages, pull request titles
  and bodies, code comments, documentation.
- **A licence header names who wrote the file, not what it is about.** A file converted
  from, or derived from, the upstream Java keeps the header it came with — Amazon's, or
  Confluent's for the `avrodata` package: the attribution is required by Apache-2.0 and
  those files are derivative works. A file this fork wrote from scratch carries
  `Copyright 2026 Mobsuccess.` above the same Apache-2.0 notice. The headers in a given
  directory are therefore deliberately not uniform; each one tracks provenance.
- **The pull request title drives the released version.** `feat!:` (or a
  `BREAKING CHANGE:` footer) is major, `feat:` is minor, anything else is a patch bump.
- Kotlin lint is ktlint 1.4.1, configured in `.editorconfig`. Local hooks:
  `pre-commit install`.

## Where the rest lives

| Topic                                                   | File                                                 |
| ------------------------------------------------------- | ---------------------------------------------------- |
| Port contract, accepted deviations from the Maven build | [docs/portage.md](docs/portage.md)                   |
| Java interop rules, and the module still in Java        | [docs/kotlin-interop.md](docs/kotlin-interop.md)     |
| Build layout, ABI dumps, integration tests              | [docs/build.md](docs/build.md)                       |
| CI, releases, branch protection, supply chain           | [docs/ci.md](docs/ci.md)                             |
| Upstream release history                                | [docs/upstream-history.md](docs/upstream-history.md) |
