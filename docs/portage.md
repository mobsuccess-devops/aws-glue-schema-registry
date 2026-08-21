# Maven → Gradle → Kotlin port

This fork starts from commit `eed1506` of `awslabs/aws-glue-schema-registry`, pushed
verbatim as its first commit. Every deviation is therefore readable with:

```bash
git diff eed1506 -- <path>
```

## The identical-behaviour contract

The port happens in two deliberately separate stages — the build system first, the
language second — so that a red test never has two possible causes.

The reference is the source repository's own test suite, measured under Maven before any
modification (JDK 17, `mvn test -pl '!integration-tests'`):

| Module                              | Tests    |
| ----------------------------------- | -------- |
| `common`                            | 139      |
| `serializer-deserializer`           | 1346     |
| `kafkastreams-serde`                | 22       |
| `avro-kafkaconnect-converter`       | 22       |
| `avro-flink-serde`                  | 20       |
| `jsonschema-kafkaconnect-converter` | 329      |
| `protobuf-kafkaconnect-converter`   | 95       |
| **Total**                           | **1973** |

The Gradle build reproduces these 1973 tests module by module, with no failure. That
total is the floor to meet again after every Kotlin conversion step.

The `main` sources were converted first, with the tests left in Java: the inherited suite
was then an oracle that had not moved, validating the converted code. The tests followed,
one module at a time, against a per-class inventory — the count of tests each class runs —
so that a class silently dropping out of the run is caught rather than hidden in a total.
Only `integration-tests`, and the Avro classes generated into the test trees, are still
Java.

## Accepted deviations from the Maven build

- **C# and `multilang-schema-registry` removed.** The Java part of that module existed
  only to expose a native library to the C# binding; with no consumer, it has no purpose.
- **`build-tools` removed.** That module only held the Checkstyle configuration of the
  Maven build, replaced by ktlint.
- **114 dormant JUnit 4 tests woken up.** `AvroDataTest` (105) and `AdditionalAvroDataTest`
  (9) were run by no engine under Maven, for lack of a `junit-vintage-engine`: they were
  silently skipped. They now run and all pass, taking `avro-kafkaconnect-converter` from
  22 to 136 tests. The code under test did not change.
- **`avro-flink-serde` points at the local module.** The pom depended on
  `schema-registry-serde` published on Maven Central (2.0.0 for compile, 1.0.2 for test)
  instead of the neighbouring module.
- **`org.lz4:lz4-java` excluded globally.** The pom excluded it from every Kafka artifact
  in favour of the `at.yawk.lz4:lz4-java` fork. Both declare the same _capability_, which
  Gradle refuses to arbitrate on its own.
- **`@NonNull`: `IllegalArgumentException` becomes `NullPointerException`.** The
  repository's root `lombok.config` sets `lombok.nonNull.exceptionType = IllegalArgumentException`,
  so the 112 `@NonNull` annotations raised an `IllegalArgumentException` on a null argument.
  Kotlin's non-nullable types raise a `NullPointerException`. Idiomatic Kotlin was chosen
  over nullable parameters guarded by `require()`, at the cost of updating the tests that
  asserted the exception type. Only the type changes: a null value is still rejected, at
  the same point.
- **Publication coordinates.** Group `com.mobsuccess` instead of `software.amazon.glue`,
  so that an artifact of this fork can never silently substitute itself for the Maven
  Central one in a consumer's dependency graph. The artifactIds are unchanged.
- **Reflection dropped in `DynamicSchema.init`.** The upstream code read
  `FileDescriptorProto.getDependencyList()` through `java.lang.reflect.Method` to work
  around a signature change in protobuf 2.6.1, wrapping any failure in a
  `RuntimeException`. The pinned protobuf version exposes the method directly, so it is
  now called directly.
- **`ProtoParser` reached through its public entry point.** `ProtobufFile` built a
  `ProtoParser` with its constructor and called `readProtoFile()`. That constructor is
  `internal` in Wire — visible from Java, not from Kotlin — so the conversion uses the
  public `ProtoParser.parse(location, data)`, which does exactly those two steps.
- **`LICENSE.txt` and `NOTICE.txt` packaged in every jar.** Apache License 2.0 §4 asks
  for both to accompany each redistributed copy of the work; the Maven build packaged
  neither, so a consumer receiving only the jar got no licence and no attribution. They
  now land in `META-INF/` of every artifact, uber-jars included. Most bundled jars carry
  files of the same name, so `shadowJar` keeps the first occurrence — the `metaInf` spec
  is the first child spec of `Jar`, which makes it ours, the one describing this work.
- **`NOTICE.txt` states the modifications.** ALv2 §4.b requires a modified distribution
  to carry prominent notice of the change. The upstream `NOTICE.txt` held the Amazon
  copyright line alone; the fork's modifications are now listed after it, along with the
  disclaimer that the fork is not endorsed by Amazon.
- **`THIRD-PARTY-LICENSES.txt` generated instead of versioned.** The inherited file
  described the dependency graph of the Maven build, a set this fork no longer has: the
  C# modules are gone, Kotlin, Wire and `at.yawk.lz4` came in. It is replaced by a
  `thirdPartyLicenses` task, built on `com.github.jk1.dependency-license-report`, which
  derives the inventory from the resolved `runtimeClasspath` and packages it into the
  uber-jars — the only artifacts that physically embed third-party code. Generating it
  per build is what keeps it from drifting a second time. The report's generation
  timestamp is stripped so that two builds of the same commit produce identical jars.
- **`AvroSchema.toString()` renders a null canonical string as `"null"`.** The Java source
  returned `canonicalString()` straight from `toString()`, and that method returns null for a
  schema carrying no Avro object. Kotlin forbids a nullable return type on `toString()`, so the
  null is rendered instead of propagated. Every caller that interpolated or concatenated the
  schema already observed `"null"`; only a direct `toString()` call differs.
- **The parsed Avro schema is memoized on the two record-rate paths.** Upstream calls
  `new Schema.Parser().parse(schemaDefinition)` once per record in
  `AWSKafkaAvroConverter.toConnectData` and in
  `GlueSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream`, re-parsing the
  same JSON for every message of a topic. Both now keep a bounded, thread-safe
  `Cache<String, Schema>` keyed on the schema definition, holding 100 entries — the size the
  other caches in this codebase already use. The registry lookup that produces the
  definition is untouched, so nothing about what is fetched changes; only the parse is
  skipped when the definition has been seen before. The parsed `Schema` is read-only for
  both callers, and it is returned as the same instance rather than an equal copy, which is
  what a consumer of a schema registry expects. This is a backport of
  [awslabs/aws-glue-schema-registry#528](https://github.com/awslabs/aws-glue-schema-registry/issues/528).
  A `LoadingCache` was deliberately not used: it wraps a loader failure in an
  `ExecutionException`, which would change the cause chain of the `DataException` and
  `AWSSchemaRegistryException` those two methods raise on a malformed schema.
- **Widened visibility on a few nested types.** `ProtobufSchemaLoaderContext` was
  `protected static` and `AvroData.FromConnectContext` was `private static`, both exposed
  through public methods — legal in Java, rejected by Kotlin. They are now public classes
  with an `internal` constructor, so they still cannot be built from outside the library.
