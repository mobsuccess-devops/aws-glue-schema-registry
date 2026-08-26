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

- **What an uber-jar bundles.** The validator reads a module's own `main` output, not the
  jar `shadowJar` assembles, so `schema-registry-serde-msk-iam.api` is four lines describing
  the one class that module declares — not the `schema-registry-serde` classes its uber-jar
  ships. That is not a gap: those classes are guarded by `:schema-registry-serde:apiCheck`,
  where they are declared. What is guarded nowhere is the _relocation and exclusion_ rules of
  `gsr.shaded-conventions`; changing those changes what consumers receive without moving a
  single dump.
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

## The uber-jars

`schema-registry-serde-msk-iam` and the three Connect converters are shaded by
`gsr.shaded-conventions`: the uber-jar replaces the main artifact, the pom is stripped of
every dependency bar `slf4j-api`, and the Gradle module metadata is disabled. They are
dropped as-is onto a Kafka Connect plugin path, where nothing resolves a transitive
dependency, so **whatever is reachable at run time has to be inside the jar**. That is the
contract, and it is what makes these four the whole weight of a publication.

Measure them with:

```bash
PACKAGE_VERSION=<version> ./gradlew clean build nmcpZipAggregation
unzip -l build/nmcp/zip/aggregation.zip
```

### What is in them

The four are the same jar with a different front end — one `schema-registry-serde`, one
Glue client, one Kafka client, and the union of the Avro, JSON Schema and Protobuf
toolchains. Taking `schema-registry-serde-msk-iam` as the representative, by originating
dependency:

| Origin                                                              |   MB | Note                                                                     |
| ------------------------------------------------------------------- | ---: | ------------------------------------------------------------------------ |
| AWS SDK v2                                                          | 15.4 | `glue` alone is 9.6 — the model classes of every Glue API, not just ours |
| Kafka compression codecs (`zstd-jni`, `snappy-java`, `at.yawk.lz4`) |  9.4 | 22 MB uncompressed, almost all native binaries for sixteen platforms     |
| `kafka-clients`                                                     |  8.8 |                                                                          |
| `mbknor-jackson-jsonschema` and `scala-library`                     |  5.8 | the JSON Schema derivation path; `scala-library` is 5.7 of it            |
| `kotlin-reflect`, `kotlin-scripting-*`, `classgraph`                |  5.8 |                                                                          |
| Netty                                                               |  3.8 | the SDK's async HTTP client                                              |
| Protobuf runtime and `proto-google-common-protos`                   |  3.4 |                                                                          |
| Apache HttpClient 4 and 5                                           |  3.2 | the SDK's two sync HTTP clients                                          |
| Guava                                                               |  2.9 |                                                                          |
| Wire                                                                |  2.7 |                                                                          |
| Jackson                                                             |  2.3 |                                                                          |
| `kotlin-stdlib`                                                     |  1.8 |                                                                          |
| everit JSON Schema and its validators                               |  1.6 |                                                                          |
| Avro                                                                |  0.6 |                                                                          |
| this repository                                                     |  0.3 |                                                                          |
| everything else                                                     |  2.1 |                                                                          |

The jar is very close to the sum of the dependency jars it repacks, so
`runtimeClasspath` is a faithful proxy: measuring a candidate exclusion does not need a
build.

### What is excluded, and why

`gsr.shaded-conventions` drops four artifacts from the four shaded modules. Nothing else in
the build is affected — `examples` and `integration-tests` depend on `schema-registry-serde`
directly and resolve the unnarrowed graph.

- **`apache-client` and `apache5-client`.** Every AWS client this library builds passes an
  explicit `UrlConnectionHttpClient`: `AWSSchemaRegistryClient` for Glue,
  `AWSKafkaAvroConverter` for the assume-role STS client. The only clients built through the
  SDK's own HTTP resolution are the ones the SDK builds for itself — the SSO credential
  providers, and the `StsClient` inside `aws-msk-iam-auth`. Since 2.29 the SDK no longer
  fails when several implementations are on the classpath: `ClasspathSdkHttpServiceProvider`
  ranks them, Apache 5 first, Apache 4 second, `HttpURLConnection` third. Bundling all three
  therefore meant those internal clients silently used Apache HttpClient 5 while everything
  the fork builds itself used `HttpURLConnection`. With the two gone the jar registers
  exactly one `SdkHttpService`, so the resolution is deterministic and the whole artifact
  speaks over one HTTP stack.
- **`netty-nio-client`**, which provides `SdkAsyncHttpService`. Nothing here builds an async
  AWS client, and no `SdkAsyncHttpService` is registered in the jar any more.
- **`wire-compiler`.** The Protobuf path uses `com.squareup.wire.schema` and
  `com.squareup.wire.Syntax`; `wire-compiler` is the code generator, and no source set
  imports it. Upstream's pom declared it at compile scope next to `wire-schema` and the port
  carried it over as `runtimeOnly`. It leaves with `swiftpoet` and the three generator
  artifacts.

The effect, measured on the aggregation bundle:

| Artifact                                 |  Before |   After |         Delta |
| ---------------------------------------- | ------: | ------: | ------------: |
| `schema-registry-serde-msk-iam`          | 69.8 MB | 61.6 MB | −8.2 (−11.8%) |
| `protobuf-kafkaconnect-converter`        | 69.8 MB | 63.1 MB |  −6.8 (−9.7%) |
| `jsonschema-kafkaconnect-converter`      | 68.8 MB | 62.0 MB |  −6.8 (−9.8%) |
| `schema-registry-kafkaconnect-converter` | 67.9 MB | 61.2 MB | −6.8 (−10.0%) |
| **one publication**                      |  284 MB |  255 MB |  −28.5 (−10%) |

`schema-registry-serde-msk-iam` gains the extra 1.5 MB because `aws-msk-iam-auth` declares
`apache-client` itself, so HttpClient 4 and `commons-codec` leave with it.

### What was measured and rejected

- **`minimize()`.** For `schema-registry-serde-msk-iam` it produces a jar of **11.9 MB that
  contains no classes at all** — no `IAMLoginModule`, no serializer, nothing. The module
  declares no source of its own, being `schema-registry-serde` plus `aws-msk-iam-auth`, so
  shadow has no root to walk from and keeps almost nothing. For the Avro converter, which
  does have classes, 67.9 MB becomes 39.0 MB by deleting nine tenths of `kafka-clients`, all
  of Netty and all of `kotlin-reflect` — every one of them loaded by name rather than
  referenced. `minimize()` cannot see a `Class.forName`, and this library resolves
  serializers, deserializers and SDK service clients by name; the GraalVM metadata in
  [native-image.md](native-image.md) exists for the same reason. Both jars pass the test
  suite.
- **The Kafka compression codecs**, 9.4 MB a jar and the second largest block after the AWS
  SDK. `kafka-clients` names `zstd-jni`, `snappy-java` and `lz4` by class from
  `org.apache.kafka.common.compress`, so they are reachable from anything holding a producer
  or a consumer. A Connect converter holds neither — the worker owns the client — which
  would make 28 MB removable from the three converters. It was not taken: the argument rests
  on a Connect worker keeping `org.apache.kafka.common` on the parent classloader, which is a
  property of a Connect version rather than of this jar, and nothing in this build or in the
  integration suite starts a worker to check it. Self-containment is the point of the
  artifact.
- **Splitting `schema-registry-serde` by format**, which is where the real weight is. Each
  converter carries the two formats it cannot use: about 19 MB of Protobuf, Wire, everit,
  `mbknor-jackson-jsonschema`, Scala and `kotlin-reflect` in the Avro converter, 13 MB in the
  Protobuf one, 6 MB in the JSON Schema one. `GlueSchemaRegistrySerializerFactory` dispatches
  on `DataFormat` and instantiates lazily, so those classes are never executed — but they are
  what makes the serde one artifact instead of three, and separating them breaks the API of
  every module and every consumer of the fork.
- **Narrowing `software.amazon.awssdk:glue`**, 9.6 MB of which the schema-registry
  operations are a rounding error. `GlueClient` declares a method per Glue operation, so
  every model class is statically reachable; there is nothing to strip that is not also a
  hand-edit of a published SDK artifact.

The arithmetic that follows is worth stating plainly: a publication drops from 284 MB to
255 MB, and Maven Central's free-tier threshold is 78 MB a month. Trimming the uber-jars is
worth doing on its own merits, and it does not change that order of magnitude — see
[ci.md](ci.md#publication).

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
  shaded modules — checked against 2.9 and 3.1.4 alike. Nothing else in the build objects, so
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
- **`serializer-deserializer` publishes a `tests` jar** consumed by `integration-tests`
  through the `testArtifacts` configuration.
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

The same three containers the workflow starts, with ports free to move — `KAFKA_BOOTSTRAP`
and `GLUE_ENDPOINT` are what the tests read. LocalStack has to stay on 4566: the Kinesis
test hard-codes that port, as the upstream source did.

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
