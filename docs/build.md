# Build

- Gradle 9.7.0, Kotlin DSL, **JVM 17** toolchain (consumable by Kafka Connect and Flink).
  Every CI job installs 17 and 21 — 21 runs Gradle itself, 17 is the compilation toolchain.
  `test-jdk` installs 25 on top, and runs the suite on 21 and 25; the order that list is
  written in matters, see [ci.md](ci.md).
- Versions centralized in `gradle/libs.versions.toml` — never hard-code a version in a
  `build.gradle.kts`
- Shared configuration in `buildSrc/src/main/kotlin/gsr.*.gradle.kts`, no `subprojects {}`
  in the root build
- Published under the `com.mobsuccess` group: releases to Maven Central, snapshots to
  GitHub Packages

## The ABI is versioned

`org.jetbrains.kotlinx.binary-compatibility-validator` is applied at the root build and
dumps the public ABI of every published module into `<module>/api/<artifactId>.api`. Those
files are committed, and `apiCheck` is wired into `check`: a change to a public signature
fails the build until the dump is refreshed.

```bash
./gradlew apiCheck    # runs as part of `check`
./gradlew apiDump     # accept the new surface, then commit the .api diff
```

This is what mechanizes the fork's promise that the API stays identical to the source. A
red `apiCheck` is not a formality — read the diff it prints and decide whether the change
is deliberate before running `apiDump`. `examples` and `integration-tests` are excluded:
they publish nothing.

The dumps include the protobuf classes generated from `src/main/proto`. They really are on
the published surface, so a protobuf bump that moves a generated signature shows up as an
ABI change, which is the point.

Two things the dumps deliberately do **not** cover:

- **What a plugin distribution ships.** The validator reads a module's own `main` output, so
  `schema-registry-serde-msk-iam.api` is four lines describing the one class that module
  declares — not the `schema-registry-serde` classes its zip carries. That is not a gap: those
  classes are guarded by `:schema-registry-serde:apiCheck`, where they are declared. What is
  guarded nowhere is the _contents_ of the zip, which follow `runtimeClasspath`: a dependency
  change moves what an operator unpacks without moving a single dump.
- **`examples`.** It is excluded because it is a sample application. It is published, so it
  technically has an ABI, but the fork makes no promise about it — a consumer depending on
  `schema-registry-examples` is doing something the module was not built for. Add it to the
  validator if that ever stops being true.

`explicitApiWarning()` is set in the Kotlin convention. It is a **warning**, not an error:
916 declarations inherited from the conversion still rely on Kotlin's implicit `public`
(780 missing a visibility modifier, 136 missing an explicit return type). The value is on
new code — anything added from now on is flagged the moment it is written.

## Coverage has a floor

`jacocoTestCoverageVerification` is wired into `check`, so a change that removes tests fails
the build the same way a broken one does. The floors are **anti-regression, not targets**:
each is the coverage the module already had on 2026-08-26, minus two to three points of
slack. Raising one is a deliberate act; lowering one to make a build pass is the thing this
guard exists to prevent.

Each module declares its own, because the spread is wide — `serializer-deserializer` sits at
69% of instructions, `kafkastreams-serde` at 100%. A single repository-wide floor would have
to be set at the weakest module and would let every other one halve unnoticed.

```kotlin
coverage {
    minimumInstructionCoverage.set(0.76)
    minimumBranchCoverage.set(0.61)
}
```

| Module                              | Instructions | floor | Branches | floor |
| ----------------------------------- | -----------: | ----: | -------: | ----: |
| `avro-flink-serde`                  |        96.5% |  0.94 |   100.0% |  0.97 |
| `avro-kafkaconnect-converter`       |        81.4% |  0.79 |    76.4% |  0.74 |
| `common`                            |        78.9% |  0.76 |    63.5% |  0.61 |
| `jsonschema-kafkaconnect-converter` |        88.8% |  0.86 |    79.2% |  0.77 |
| `kafkastreams-serde`                |       100.0% |  0.97 |        — |     — |
| `protobuf-kafkaconnect-converter`   |        95.8% |  0.93 |    88.4% |  0.86 |
| `serde-kotlin`                      |        87.9% |  0.85 |    57.9% |  0.55 |
| `serializer-deserializer`           |        68.9% |  0.66 |    60.0% |  0.58 |

A module that declares nothing gets the convention default, 0.60 of instructions and 0.50 of
branches, so a new module is guarded before anyone measures it. `examples` and
`integration-tests` set `enabled` to `false`, the same two the ABI validator ignores: one is
a sample application, the other holds no `main` source at all.
`schema-registry-serde-msk-iam` needs no exclusion — it has no tests, so the task is skipped
for want of execution data; add a test there and the default floor applies.

Two counters, not four. `LINE` tracks `INSTRUCTION` closely enough to add nothing, and
`METHOD`/`CLASS` move in whole units, so a small module crosses them in jumps that say
little about what the tests actually check.

Re-measure with `./gradlew jacocoTestReport` and read
`<module>/build/reports/jacoco/test/jacocoTestReport.xml` — the `INSTRUCTION` and `BRANCH`
counters of the root `<report>` element are what the rules compare against.

The `madrapps/jacoco-report` step in `ci.yml` comments the numbers on a pull request and is
**not** a gate — the action has no failure mode. Its `min-coverage-*` inputs are set to 66,
the weakest floor any module enforces, so its verdict cannot contradict the one that
actually blocks a merge.

## The plugin distributions

`schema-registry-serde-msk-iam` and the three Connect converters publish an ordinary jar with
a complete pom, like every other module. They also build a **plugin distribution**: a zip of
that jar and its whole runtime classpath, laid out as a Kafka Connect plugin directory.

```bash
./gradlew pluginDistribution                       # all four
./gradlew :schema-registry-kafkaconnect-converter:pluginDistribution
```

Each zip holds one directory, `<artifactId>-<version>/`, with every jar under `lib/` and
`LICENSE.txt`, `NOTICE.txt` and `THIRD-PARTY-LICENSES.txt` beside it. That is the layout a
Connect worker walks and the one Confluent Hub packages use. `gsr.distribution-conventions`
builds it, wires it into `assemble`, and deliberately does **not** add it to any publication:
the zips are attached to the GitHub Release, not pushed to Maven Central or GitHub Packages.
See [ci.md](ci.md#publication).

### Why they are not uber-jars any more

Until 4.0.0 these four modules replaced their main artifact with a shaded uber-jar and
published a pom stripped of every dependency bar `slf4j-api`. That inherited upstream's empty
`maven-shade-plugin` configuration, and it cost:

|                                          | Before (uber-jars) | After (thin jars + zips) |
| ---------------------------------------- | -----------------: | -----------------------: |
| one publication                          |  284 MB, 188 files |    **7.9 MB, 204 files** |
| `schema-registry-serde-msk-iam`          |            69.8 MB |                   6.3 KB |
| `protobuf-kafkaconnect-converter`        |            69.8 MB |                  95.3 KB |
| `jsonschema-kafkaconnect-converter`      |            68.8 MB |                 115.0 KB |
| `schema-registry-kafkaconnect-converter` |            67.9 MB |                  91.8 KB |

Size was the trigger — Maven Central meters the free tier against a 78 MB monthly release-size
threshold — but it is not the only thing that was wrong with the old shape. A pom that declares
nothing cannot be arbitrated: a consumer could not override a Kafka or Jackson version, and a
CVE scanner reading the pom saw an artifact with one dependency. The bytes moved rather than
disappeared; what changed is that they moved to a channel that is anonymous, unmetered, and
honest about being a distribution rather than a library.

### What was inside them, and what was tried first

Worth recording, because the same content is what an operator now unpacks. By originating
dependency, for `schema-registry-serde-msk-iam` — the four were within 3 MB of each other:

| Origin                                                              |   MB |
| ------------------------------------------------------------------- | ---: |
| AWS SDK v2 (`glue` alone is 9.6)                                    | 15.4 |
| Kafka compression codecs — `zstd-jni`, `snappy-java`, `at.yawk.lz4` |  9.4 |
| `kafka-clients`                                                     |  8.8 |
| `mbknor-jackson-jsonschema` and `scala-library`                     |  5.8 |
| `kotlin-reflect`, `kotlin-scripting-*`, `classgraph`                |  5.8 |
| Netty, Apache HttpClient 4 and 5 — the AWS SDK's other HTTP clients |  7.0 |
| Protobuf and `proto-google-common-protos`                           |  3.4 |
| Guava                                                               |  2.9 |
| Wire                                                                |  2.7 |
| Jackson                                                             |  2.3 |
| `kotlin-stdlib`                                                     |  1.8 |
| everit JSON Schema and its validators                               |  1.6 |
| Avro                                                                |  0.6 |
| this repository                                                     |  0.3 |
| everything else                                                     |  2.1 |

Three narrower fixes were measured before the shape changed, and none of them was enough:

- **Dropping the AWS SDK HTTP clients the fork never selects.** Every AWS client this library
  builds passes an explicit `UrlConnectionHttpClient`, so `apache-client`, `apache5-client` and
  `netty-nio-client` are dead weight — except that since SDK 2.29 the classpath provider ranks
  implementations rather than failing on several, Apache 5 first, so the clients the SDK builds
  for itself silently used HttpClient 5. Worth −28 MB across the four jars, or −10%.
- **`minimize()`, which cannot work here.** For `schema-registry-serde-msk-iam` it produces a
  jar of 11.9 MB **containing no classes at all** — the module declares only a placeholder, so
  shadow has no root to walk from. For the Avro converter, 67.9 MB becomes 39.0 MB by deleting
  nine tenths of `kafka-clients`, all of Netty and all of `kotlin-reflect`, every one of them
  loaded by name. Both jars pass the full test suite, which is exactly the trap.
- **Dropping the Kafka compression codecs from the three converters**, 9.4 MB each. A converter
  never holds a producer or a consumer, so it never compresses — but the argument rests on the
  worker's plugin classloader keeping `org.apache.kafka.common` parent-first, and nothing here
  starts a worker to check it.

Two structural facts remain true of what an operator unpacks, and neither is worth an API break
now that the bytes are off Central: each converter carries the two data formats it cannot use
(about 19 MB in the Avro one, 13 MB in the Protobuf one, 6 MB in the JSON Schema one), because
`schema-registry-serde` is one artifact for all three; and `software.amazon.awssdk:glue` is
9.6 MB of which the schema-registry operations are a rounding error, because `GlueClient`
declares a method per Glue operation and every model class is statically reachable from it.

## Other build notes

- **The jars carry GraalVM reachability metadata.**
  `serializer-deserializer` and `kafkastreams-serde` each ship a
  `META-INF/native-image/com.mobsuccess/<artifactId>/` directory that `native-image` reads
  off the classpath. `reflect-config.json` registers the classes a Kafka or Kafka Streams
  configuration names as a string; `resource-config.json` lists the 29 `.proto` files
  `ProtobufSchemaLoader` loads with `getResourceAsStream`. **That list has to move with the
  loader**: adding a proto to `GOOGLE_API_PROTOS`, `GOOGLE_WELLKNOWN_PROTOS` or
  `WIRE_PROTOS` without adding it to `resource-config.json` compiles, passes every test on
  the JVM, and throws `IOException: Proto file not found` only inside a native image. The
  metadata is verified by building a native image of a consumer and comparing the
  `registered for reflection` and `resources` counts of the build output, not by any test in
  this repository.
- **Lombok is gone.** It survived only in `integration-tests`, the last module in Java, and
  went with it: the dependency, the root `lombok.config`, and the `lombok` entry in
  `libs.versions.toml`. Nothing in the build declares an annotation processor any more.
- **Root `gradle.properties`** turns on parallel execution and the build cache, and raises
  the daemon heap: the 512m default is inherited by the Kotlin compiler daemon and is not
  enough to compile the test sources of `serializer-deserializer`.
- **Configuration caching is not enabled.** `com.github.jk1.dependency-license-report` holds a
  `Project` reference in its `ReportTask`, so the entry is discarded on every build of the four
  modules that build a plugin distribution — checked against 2.9 and 3.1.4 alike. Nothing else in the build objects, so
  `org.gradle.configuration-cache=true` can go into `gradle.properties` the day that task is
  fixed.
- **Archives are reproducible**: `isPreserveFileTimestamps = false` and
  `isReproducibleFileOrder = true` on every `AbstractArchiveTask`, so two builds of the same
  commit produce byte-identical jars and a cached jar is the same artifact as a fresh one.
- **`org.lz4:lz4-java` is excluded globally** in favour of `at.yawk.lz4:lz4-java`. Both
  declare the same _capability_; reintroducing the former breaks resolution.
- **Code generation**: protobuf (`serializer-deserializer`, `protobuf-kafkaconnect-converter`)
  and Avro (`avro-kafkaconnect-converter`). Generated sources are not versioned.
- **protobuf has two versions, and the lower one is the contract.** Generated protobuf code
  stamps the protoc version that produced it and refuses, at class initialization, a runtime
  older than itself. The `protoc` this build uses is therefore a _floor_ imposed on every
  consumer: one whose own BOM pins protobuf lower cannot load
  `metadata.ProtobufSchemaMetadata` at all, and has no fix beyond forcing the version — which
  is what a Quarkus consumer, pinned by an `enforcedPlatform`, had to do. `protobufGencode` is
  that floor, and it is not a free choice: it is the highest gencode present anywhere in what
  this build publishes or drags, and a third-party jar sets it as surely as our own protoc
  does. Today `proto-google-common-protos` 2.73.0 holds it up — built against 4.33.2, pulled
  into `protobuf-kafkaconnect-converter` by apicurio — and below that
  `FileDescriptorUtils.baseDependencies` dies on `NoClassDefFoundError:
com/google/protobuf/GeneratedFile`. Otherwise the floor moves up only when no consumer is
  left below it, and the fork gains nothing from generating with a newer protoc.
  `protobufRuntime` is what the build declares as a dependency, so a consumer with no
  constraint of its own resolves the current release, while a consumer pinned anywhere
  between the floor and it resolves its own pin — a plain `require` is a minimum that a
  platform may lower, not a lock.
- **`require` plus `prefer` does not express that**, which is worth knowing before reaching
  for it: a `prefer` is ignored as soon as the same constraint carries a `require`, so
  `require = floor, prefer = current` resolves to the floor everywhere, including for
  consumers.
- **Every `*CompileClasspath` is forced to the floor**, so the fork cannot compile against an
  API its floor does not have, while tests and the published metadata keep the current
  runtime. `./gradlew test -PprotobufFloor` forces the whole graph down to the floor instead;
  that is what the `Tests on the protobuf floor` job runs. It catches a dependency bump that
  drags in gencode newer than the floor — which is how the 4.33.2 above was found — but not
  the floor itself being raised, since raising it makes the job agree with itself. That is
  what the `com.google.protobuf:protoc` entry of `dependabot.yml` is for: the floor is a
  promise made to consumers, so it moves by decision, never by a dependency bump.
- **`serializer-deserializer` builds a `tests` jar, and does not publish it.**
  `integration-tests` consumes it through the `testArtifacts` configuration, which is a
  project dependency and needs no publication. See [portage.md](portage.md).
- The Kotlin dependencies pulled in by `mbknor-jackson-jsonschema` and `wire` are pinned at
  `1.9.25` (`kotlinRuntime` in the catalog): that is distinct from the compiler version.

## Integration tests

`*IntegrationTest` classes stay out of the unit run: `tasks.test` excludes them, `check`
never pulls them in, and `./gradlew build` reports the same test count as before. They are
reached through `integrationTest`, a separate task the convention plugin registers per
module, and driven by `.github/workflows/integration.yml` — nightly and on demand, never on
a pull request.

What each of them needs:

| Task                                                            | Needs                           |
| --------------------------------------------------------------- | ------------------------------- |
| `:schema-registry-kafkaconnect-converter:integrationTest`       | nothing                         |
| `:schema-registry-integration-tests:integrationTestWithoutGlue` | a Kafka broker and LocalStack   |
| `:schema-registry-integration-tests:integrationTest`            | the above, plus a Glue endpoint |

The endpoints are read from the environment, so a runner can point them anywhere:
`GLUE_ENDPOINT` for Glue and `KAFKA_BOOTSTRAP` for the broker; unset, they fall back to the
values the upstream sources hard-coded. The region needs no override of its own — it comes
from the AWS SDK provider chain, which reads `AWS_REGION`.

The workflow supplies the Glue endpoint itself, as a `motoserver/moto` service container, so
the suite runs whole without an AWS account. LocalStack is not an option for this: it serves
Glue only in its top paid tier, and the Schema Registry it offers there is AVRO-only, while
these tests cover Avro, JSON Schema and Protobuf alike. Setting the `GLUE_ENDPOINT`
repository variable overrides the emulator and points the same tests at a real endpoint.

### Running the suite locally

`integration-tests/docker-compose.yml` holds the workflow's three services, so one command
brings up the same stack the nightly run faces:

```bash
docker compose -f integration-tests/docker-compose.yml up -d
```

The file is kept in step with `.github/workflows/integration.yml` — same images, same
versions, same environment, same health checks — and it is the only reason to prefer it over
the explicit `docker run` lines below: a local failure that the workflow does not reproduce is
worth chasing, and it is only worth chasing when the two stacks are the same.

The equivalent by hand, with ports free to move — `KAFKA_BOOTSTRAP` and `GLUE_ENDPOINT` are
what the tests read. LocalStack has to stay on 4566: the Kinesis test hard-codes that port, as
the upstream source did.

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:3.9.1
docker run -d --name localstack -p 4566:4566 \
  -e SERVICES=kinesis,dynamodb,cloudwatch,sts -e EAGER_SERVICE_LOADING=1 localstack/localstack:3.8
docker run -d --name moto -p 5000:5000 motoserver/moto:5.2.2
```

```bash
AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  GLUE_ENDPOINT=http://localhost:5000 \
  ./gradlew :schema-registry-integration-tests:integrationTest
```

A whole run takes about thirty-five minutes and is **73 tests, none failing** — 45 in
`GlueSchemaRegistryKafkaIntegrationTest`, 28 in `GlueSchemaRegistryKinesisIntegrationTest`.
That is the number to match: the suite is invisible to `build`, so nothing else catches a
class that stops running. Most of the wall clock is the fifteen KPL/KCL cases: the KCL 3
scheduler takes about a hundred seconds to reach its worker loop against LocalStack.

`GlueSchemaRegistryKinesisIntegrationTest` creates a Kinesis stream per test — 28 of them,
each with its own KCL lease table — and deletes none of them. The workflow gets a fresh
LocalStack container every run, so it never notices; a container reused locally hits
LocalStack's 100-shard account limit after the fourth run and every later test fails with
`LimitExceededException`. Delete the streams and the DynamoDB tables between runs, or
restart the container.
