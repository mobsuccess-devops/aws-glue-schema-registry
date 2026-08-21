# Build

- Gradle 9.7.0, Kotlin DSL, **JVM 17** toolchain (consumable by Kafka Connect and Flink).
  Every CI job installs 17 and 21 — 21 runs Gradle itself, 17 is the compilation toolchain.
  `test-jdk` installs 25 on top, and runs the suite on 21 and 25; the order that list is
  written in matters, see [ci.md](ci.md).
- Versions centralized in `gradle/libs.versions.toml` — never hard-code a version in a
  `build.gradle.kts`
- Shared configuration in `buildSrc/src/main/kotlin/gsr.*.gradle.kts`, no `subprojects {}`
  in the root build
- Published to GitHub Packages under the `com.mobsuccess` group

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

A whole run takes about twenty minutes and is **73 tests, none failing** — 45 in
`GlueSchemaRegistryKafkaIntegrationTest`, 28 in `GlueSchemaRegistryKinesisIntegrationTest`.
That is the number to match: the suite is invisible to `build`, so nothing else catches a
class that stops running.
