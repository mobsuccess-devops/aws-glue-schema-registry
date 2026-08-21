# Upstream release history

This fork does not keep a changelog. What changed in each of **its** releases is on the
[releases page](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases), and
the deliberate deviations from upstream behaviour are in [portage.md](portage.md).

This file exists for the part neither of those can hold: the release history of
[`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry)
from before the fork existed. Those releases were published on Maven Central as
`software.amazon.glue`; none of them is tagged in this repository, so none of them can ever
be a release here.

## Where the fork starts

This repository's first commit is upstream's
[`eed1506`](https://github.com/awslabs/aws-glue-schema-registry/commit/eed1506), pushed
verbatim — it was upstream's `master` at the time. Every deviation since is therefore
readable with `git diff eed1506 -- <path>`.

`eed1506` sits **after** upstream's 1.1.27 release. The fork consequently ships upstream work
that upstream itself has never released — most visibly the JSON `className` allowlist
([awslabs #533](https://github.com/awslabs/aws-glue-schema-registry/pull/533)) and the
cross-account `assumeRole` support in the Avro converter
([awslabs #376](https://github.com/awslabs/aws-glue-schema-registry/pull/376)). That is why a
consumer coming from `software.amazon.glue:1.1.27` meets the `className` change as a breaking
change in this fork's 2.0.0: it is inherited, not invented here. See
[Deserializing JSON into a Java POJO](usage.md#deserializing-json-into-a-java-pojo-classname-resolution).

Upstream has not moved since 2026-08-13.

## Two 1.0.0 releases

The fork restarted its numbering at 1.0.0, so **`1.0.0` is ambiguous on its own**: there is
upstream's, at the bottom of this file, and this fork's, released 2026-08-18. The group id is
what tells them apart — `software.amazon.glue` against `com.mobsuccess` — which is precisely
why it differs. An artifact from this fork can never silently substitute for the Maven Central
one in a consumer's dependency graph.

## Upstream releases — `software.amazon.glue`

Reproduced as AWS wrote them, newest first. They carry no dates because the upstream changelog
never had any.

### 1.1.27

- Introduce lz4 shim and dependency upgrade to fix vulnerabilities
- Updated local integration tests to make requests to local stack syncrounysly to correct for flakyness

### 1.1.26

- Introduces multilang support for csharp clients

### 1.1.25

- Upgraded aws-sdk version to fix vulnerabilities

### 1.1.24

- Upgraded square-wireschema version to fix vulnerabilities

### 1.1.23

- Upgraded json-schema dependencies version to fix vulnerabilities

### 1.1.22

- Upgraded protobuf dependencies version to fix vulnerabilities

### 1.1.21

- Upgraded Avro dependencies version to fix vulnerabilities

### 1.1.20

- Upgrade the dependency version to remove commons:compress dependency

### 1.1.19

- Upgraded dependency versions to remove ION dependencies

### 1.1.18

- Add a dummy class in the serializer-deserializer-msk-iam module for javadoc and source jar generation
- Upgraded Avro and Json dependencies version
- Upgraded AWS SDK v1 and v2 versions to fix vulnerabilities

### 1.1.17

- Upgraded kafka dependencies version

### 1.1.16

- Upgraded Wire version
- Excluded some transitive dependencies that are having vulnerabilities

### 1.1.15

- Upgrade Avro, Apicurio and Localhost utils versions

### 1.1.14

- Upgraded Protobuf dependency version to prevent a CVE
- Upgraded everit-json-schema dependency version to prevent a CVE

### 1.1.13

- Upgraded kotlin dependency versions to prevent a CVE

### 1.1.12

- Upgraded Avro Version to prevent a CVE

### 1.1.11

- Add support for Kafka Connect Protobuf converter

### 1.1.10

- Fix bug for missing Protobuf wellknown types
- Fix Json schema converter NPEs due to missing connect.index and connect.type for sink only cases
- Add AWS SDK dependency to allow irsa service account

### 1.1.9

- Added Support for Protobuf Format
- Improved the caching mechanism to improve availability of the serializer and deserializer

### 1.1.5

- Fix security vulnerability in transitive dependencies
- Remove configuration logging information

### 1.1.4

- Upgrade Apache Kafka version to 2.8.1

### 1.1.3

- Modify UserAgent to emit usage metrics
- Add tests to include key and value schemas both

### 1.1.2

- Introduce cache to improve serialization performance
- Add DatumReader Cache to improve de-serialization performance
- Reduce logging
- Add additional examples of configuring Kafka Connect and clarification on what property names are expected
- Fix resource clean up in Kafka integration test

### 1.1.1

- Fixed checkstyle errors with maven build in integration-tests folder.
- Reduced number of Canaries tests.
- Removed jitpack as a repo for everit and using maven central to pull everit.

### 1.1.0

- Added Support for JSONSchema Format.
- Added Validation logic while using encode method for calls through KPL.
- Generalized Kafka Specific Serializer/Deserializer to a data format agnostic classes like
  GlueSchemaRegistryKafkaSerializer/GlueSchemaRegistryKafkaDeserializer.
- Generalized AWSKafkaAvroSerDe to GlueSchemaRegistryKafkaSerDe for it to be used for multiple data formats.
- Using better convention for poms and maven inheritance.
- Added JSON Kafka Converter.
- Improved integration tests to run with local dockerized streaming systems.

### 1.0.1

- Added more documentation
- Reduced logging
- Added flexibility to schema naming
- Added Kinesis Data Streams usage examples
- Added integration tests

### 1.0.0

- Initial Release
