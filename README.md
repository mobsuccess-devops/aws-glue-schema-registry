# AWS Glue Schema Registry Library

[![JVM Library](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/mobsuccess-devops/aws-glue-schema-registry?sort=semver&label=release&color=blue)](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases/latest)
[![Apache 2 License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.txt)
![JVM 17](https://img.shields.io/badge/JVM-17-blue.svg)

Mobsuccess fork of [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry).

**AWS Glue Schema Registry** lets you centrally discover, control and evolve schemas while
ensuring produced data was validated against a registered schema. This library provides the
serializers and deserializers that plug into it — for Kafka, Kafka Streams, Kafka Connect and
Kinesis — in AVRO, JSON Schema and Protobuf.

- Records are serialized producer-side and deserialized consumer-side through
  `schema-registry-serde`.
- Three data formats: AVRO, JSON (via [JSON Schema](https://json-schema.org/) Draft04,
  Draft06 and Draft07) and Protocol Buffers (syntax 2 and 3).
- Optional auto-registration of new schemas, with an evolution check on registration.
- Optional record compression, and a built-in in-memory cache — the schema version id
  producer-side, the schema itself consumer-side.
- A migration path from a third-party schema registry.

## How this fork differs

The library behaviour is unchanged — the inherited test suite passes in full, and the first
commit of this repository is the upstream source verbatim, so every deviation is visible with
`git diff eed1506`. What changed is everything around the code:

|              | Upstream               | This fork                          |
| ------------ | ---------------------- | ---------------------------------- |
| Build        | Maven                  | Gradle 9.7.0, Kotlin DSL           |
| Languages    | Java + C#              | Java only, Kotlin port in progress |
| Distribution | Maven Central          | GitHub Packages                    |
| Group        | `software.amazon.glue` | `com.mobsuccess`                   |
| JVM target   | 8                      | 17                                 |

The C# binding and its native (GraalVM) layer were removed: without a binding, the native
layer had no consumer. Artifact names are unchanged. The group differs on purpose, so that an
artifact from this fork can never silently substitute for the Maven Central one.

Every deviation is documented in [docs/portage.md](docs/portage.md); agent-facing notes live
in [AGENTS.md](AGENTS.md).

## Packages

| Artifact                                 | Purpose                                       |
| ---------------------------------------- | --------------------------------------------- |
| `schema-registry-common`                 | Glue client, cache, exceptions                |
| `schema-registry-serde`                  | Core SerDe (AVRO, JSON Schema, Protobuf)      |
| `schema-registry-serde-kotlin`           | Kotlin configuration DSL and typed `Serde<T>` |
| `schema-registry-serde-msk-iam`          | Uber-jar bundling the SerDe with MSK IAM auth |
| `schema-registry-kafkastreams-serde`     | Kafka Streams integration                     |
| `schema-registry-kafkaconnect-converter` | Kafka Connect AVRO converter                  |
| `jsonschema-kafkaconnect-converter`      | Kafka Connect JSON Schema converter           |
| `protobuf-kafkaconnect-converter`        | Kafka Connect Protobuf converter              |
| `schema-registry-flink-serde`            | Flink serialization schemas                   |

Most applications only need `schema-registry-serde`. The four Kafka Connect / MSK IAM
artifacts are shaded uber-jars, meant to be dropped onto a Connect plugin path.

## Installation

You need an AWS account with the [Glue Schema Registry set up](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-gs.html),
[credentials the AWS SDK can resolve](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html),
and JVM 17 or later.

Artifacts are published to **GitHub Packages**, which requires a GitHub personal access token
(classic, scope `read:packages`) even to read a public repository. Export it, then declare the
repository:

```bash
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

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

The latest version is on the
[releases page](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases/latest).

Maven and Groovy DSL setups, CI credentials, the version compatibility matrix, the move from
the `software.amazon.glue` artifact and a troubleshooting table are in
**[docs/installation.md](docs/installation.md)**.

## Basic usage

Producing Avro records to Kafka takes two properties beyond the usual producer configuration
— the serializer class and the data format:

```kotlin
properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaSerializer::class.java.name
properties[AWSSchemaRegistryConstants.DATA_FORMAT] = DataFormat.AVRO.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.REGISTRY_NAME] = "my-registry"
properties[AWSSchemaRegistryConstants.SCHEMA_NAME] = "my-schema"

KafkaProducer<String, GenericRecord>(properties).use { producer ->
    producer.send(ProducerRecord(topic, record.get("id").toString(), record))
}
```

The consumer reads the format from the record header, so it only needs the deserializer:

```kotlin
properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = GlueSchemaRegistryKafkaDeserializer::class.java.name
properties[AWSSchemaRegistryConstants.AWS_REGION] = "us-east-1"
properties[AWSSchemaRegistryConstants.AVRO_RECORD_TYPE] = AvroRecordType.GENERIC_RECORD.getName()

KafkaConsumer<String, GenericRecord>(properties).use { consumer ->
    consumer.subscribe(listOf(topic))
    while (true) {
        consumer.poll(Duration.ofMillis(100)).forEach { record -> process(record.value()) }
    }
}
```

The schema has to exist in the registry, unless auto-registration is enabled. JSON and
Protobuf, POJOs, compression, caching and the rest are in
**[docs/usage.md](docs/usage.md)**.

## Documentation

| Document                                         | What is in it                                                                  |
| ------------------------------------------------ | ------------------------------------------------------------------------------ |
| [Installation](docs/installation.md)             | GitHub Packages setup, compatibility matrix, migration from the AWS artifact   |
| [Usage](docs/usage.md)                           | Kafka producers and consumers for the three formats, Kinesis, Kafka Streams    |
| [Kotlin DSL](serde-kotlin/README.md)             | `schema-registry-serde-kotlin`: the configuration DSL and the typed `Serde<T>` |
| [Configuration reference](docs/configuration.md) | Every property, its default and the side that reads it                         |
| [Kafka Connect](docs/kafka-connect.md)           | The three converters, plugin path, worker configuration, cross-account role    |
| [Flink](docs/flink.md)                           | The Flink serialization schemas, and why they are not recommended              |
| [Build](docs/build.md)                           | The Gradle build: toolchain, conventions, ABI dumps, code generation           |
| [CI and supply chain](docs/ci.md)                | Releases, workflow permissions, pinning, dependency policy                     |
| [Java interop](docs/kotlin-interop.md)           | Where Kotlin and Java do not line up, for anyone writing Kotlin here           |
| [Port notes](docs/portage.md)                    | Maven → Gradle → Kotlin, and the accepted deviations from upstream             |
| [Upstream history](docs/upstream-history.md)     | Releases of `awslabs/aws-glue-schema-registry` from before the fork            |
| [Upstream tracking](docs/upstream-tracking.md)   | Every open upstream issue and pull request, and how this fork relates to it    |
| [Contributing](CONTRIBUTING.md)                  | Building, testing, house style, pull requests, releases                        |

## Building from source

```bash
./gradlew clean build     # compile, run the full test suite, produce the jars
./gradlew test            # tests only
./gradlew assemble        # jars only
```

Gradle resolves a JVM 17 toolchain on its own; no local Maven or JDK pinning is needed.

The `*IntegrationTest` classes need a Kafka broker and a Glue endpoint. They stay out of the
unit run, are reached through the separate `integrationTest` task, and run nightly through
[integration.yml](.github/workflows/integration.yml), never on a pull request. House rules
for contributions are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Reporting a security issue

Vulnerabilities in this fork are **not** reported to AWS: the build, the dependency set and
the Kotlin conversion are specific to it, and upstream is dormant. Use GitHub's private
vulnerability reporting on this repository —
[Report a vulnerability](https://github.com/mobsuccess-devops/aws-glue-schema-registry/security/advisories/new)
— and please do not open a public issue. The full policy is in [SECURITY.md](SECURITY.md).

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md) for the house
rules and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License and attribution

This library is distributed under the [Apache License 2.0](LICENSE.txt), unchanged from
upstream. This fork is a **modified** distribution of
[`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry):
the modifications are listed in [NOTICE.txt](NOTICE.txt) and summarised in
[How this fork differs](#how-this-fork-differs). It is neither endorsed by nor affiliated
with Amazon.com, Inc. or its affiliates.

Every published artifact carries `META-INF/LICENSE.txt` and `META-INF/NOTICE.txt`. The
uber-jars — the Kafka Connect converters and `schema-registry-serde-msk-iam` — bundle their
dependencies, and so also carry `META-INF/THIRD-PARTY-LICENSES.txt`, an inventory generated
from the resolved runtime classpath at build time:

```bash
./gradlew thirdPartyLicenses
```
