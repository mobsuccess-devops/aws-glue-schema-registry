# AWS Glue Schema Registry Library

[![JVM Library](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/mobsuccess-devops/aws-glue-schema-registry/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/com.mobsuccess/schema-registry-serde?label=Maven%20Central&color=blue)](https://central.sonatype.com/artifact/com.mobsuccess/schema-registry-serde)
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

The contract is **identical by default, better where documented**. The library behaves like
its upstream source at the Java level — that is what makes the artifact a drop-in replacement
— and the improvements the fork carries are documented deviations, never silent ones: every
one of them is written up in [docs/portage.md](docs/portage.md). The inherited test suite is
the oracle and it stays green, and the first commit of this repository is the upstream source
verbatim, so `git diff eed1506` shows every change.

|              | Upstream                                | This fork                               |
| ------------ | --------------------------------------- | --------------------------------------- |
| Build        | Maven                                   | Gradle 9.7.0, Kotlin DSL                |
| Languages    | Java + C#                               | Kotlin, tests included                  |
| Distribution | Maven Central                           | Maven Central (snapshots on GH)         |
| Group        | `software.amazon.glue`                  | `com.mobsuccess`                        |
| JVM target   | 8                                       | 17                                      |
| Dependencies | Kafka 3.6.1, Wire 5.2.0, Jackson 2.12.2 | Kafka 3.9.2, Wire 6.4.6, Jackson 2.22.2 |

What the fork adds on top of the port:

- **Dependencies that keep moving.** Upstream's are where its last release left them.
  Dependabot watches `gradle/libs.versions.toml`, and the _resolved_ runtime graph — the
  transitives, including the ones shaded into the uber-jars, which is where most CVEs sit —
  is submitted to GitHub so that its alerts see them too. A transitive that carries one is
  constrained out rather than waited on — `scala-library` is held above CVE-2022-36944.
  protobuf 4.36.0 is on `master` and ships in the next major; the current release still
  resolves 3.25.5.
- **GraalVM native-image support.** `schema-registry-serde` and
  `schema-registry-kafkastreams-serde` carry their own reachability metadata in
  `META-INF/native-image`, verified by building a real Quarkus consumer as a native image, so
  a native consumer writes no configuration for this library —
  [docs/native-image.md](docs/native-image.md).
- **A Kotlin API.** `schema-registry-serde-kotlin` adds a typed configuration DSL and a
  `Serde<T>` that checks the type of each record instead of handing Kafka Streams a
  `Serde<Any>`. Strictly additive: no other module depends on it —
  [serde-kotlin/README.md](serde-kotlin/README.md).
- **Kafka Connect configuration that validates.** The three converters declare every registry
  property in a `ConfigDef`, so `PUT /connector-plugins/{plugin}/config/validate` and a
  Connect UI have something to work with, and an impossible value is rejected when the
  connector is created rather than at the first record.
- **An opt-in JSON Schema compatibility check.** Glue enforces a schema's compatibility mode
  for AVRO and Protobuf, but not for JSON. `jsonSchemaCompatibilityCheckEnabled` compares the
  `required` contract with the latest registered version producer-side, so a breaking version
  fails where it is produced rather than in a consumer.
- **A nightly integration suite.** The `*IntegrationTest` classes run every night against a
  real Kafka broker — once with LocalStack standing in for AWS, once against a real Glue
  registry — instead of only on a developer's machine.

The C# binding was removed, and with it `native-schema-registry`: a C shared library, compiled
ahead of time from the Java, whose only purpose was to give that binding an entry point.
Removing it says nothing about running this library _inside_ a GraalVM native image, which is
supported and documented in [docs/native-image.md](docs/native-image.md). Artifact names are
unchanged. The group differs on purpose, so that an artifact from this fork can never silently
substitute for the Maven Central one.

Agent-facing notes live in [AGENTS.md](AGENTS.md).

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

Releases are on **Maven Central** under the `com.mobsuccess` group — no repository block, no
token:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("com.mobsuccess:schema-registry-serde:<version>")
}
```

The latest version is on
[Maven Central](https://central.sonatype.com/artifact/com.mobsuccess/schema-registry-serde) and
on the [releases page](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases/latest).
Snapshots are a separate channel: every push to `master` publishes `<next-version>-SNAPSHOT` to
GitHub Packages, which does require a token.

Maven and Groovy DSL setups, the snapshot channel, the version compatibility matrix, the move
from the `software.amazon.glue` artifact or from GitHub Packages, and a troubleshooting table
are in **[docs/installation.md](docs/installation.md)**.

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
| [Installation](docs/installation.md)             | Maven Central coordinates, snapshot channel, compatibility matrix, migration   |
| [Usage](docs/usage.md)                           | Kafka producers and consumers for the three formats, Kinesis, Kafka Streams    |
| [Kotlin DSL](serde-kotlin/README.md)             | `schema-registry-serde-kotlin`: the configuration DSL and the typed `Serde<T>` |
| [Configuration reference](docs/configuration.md) | Every property, its default and the side that reads it                         |
| [Kafka Connect](docs/kafka-connect.md)           | The three converters, plugin path, worker configuration, cross-account role    |
| [Native image](docs/native-image.md)             | GraalVM: what the jars declare, what a consumer still declares                 |
| [Flink](docs/flink.md)                           | The Flink serialization schemas, and why they are not recommended              |
| [Build](docs/build.md)                           | The Gradle build: toolchain, conventions, ABI dumps, code generation           |
| [CI and supply chain](docs/ci.md)                | Releases, workflow permissions, pinning, dependency policy                     |
| [Java interop](docs/kotlin-interop.md)           | Where Kotlin and Java do not line up, for anyone writing Kotlin here           |
| [Port notes](docs/portage.md)                    | Maven → Gradle → Kotlin, and the accepted deviations from upstream             |
| [Upstream history](docs/upstream-history.md)     | Releases of `awslabs/aws-glue-schema-registry` from before the fork            |
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
