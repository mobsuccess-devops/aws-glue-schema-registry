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

`integration-tests` came last, and had no such oracle: its classes are excluded from
`tasks.test`, so a green build proves only that they compile. The conversion was measured
instead against a full run of the suite on an emulator stack — Kafka in KRaft mode,
LocalStack for Kinesis, DynamoDB and CloudWatch, moto for Glue — recording the same
per-test inventory before and after. Java and Kotlin both run **73 tests, none failing**,
with an identical breakdown per test method. The recipe is in [build.md](build.md).

The only Java left in the repository is the Avro classes generated into the test trees.

## Accepted deviations from the Maven build

- **`templating-maven-plugin` replaced by a `Copy` task.** `common` declares
  `generateVersionSource`, which expands `src/main/java-templates` into the build directory
  so that `MavenPackaging.VERSION` carries the project version. That constant is read by the
  User-Agent interceptor of the Glue calls, so the value has to be the built version rather
  than a literal.
- **C# and `multilang-schema-registry` removed.** The Java part of that module existed
  only to expose a native library to the C# binding; with no consumer, it has no purpose.
- **`build-tools` removed.** That module only held the Checkstyle configuration of the
  Maven build, replaced by ktlint.
- **Avro 1.11.4 → 1.12.1.** The pom pins `avro.version` at 1.10.2 and the fork had moved to
  1.11.4 for the 1.11 line's security patches; 1.12.1 is the current line. Nothing under
  `src/` changes and the inherited suite runs green untouched — the one thing that made the
  bump impossible was the `org.apache.avro.Schemas` copy removed in the entry below, which
  reached for internals 1.12 reworked.

  **1.12.2 is deliberately not taken.** It adds `org.apache.avro.util.ClassSecurityValidator`,
  a default-deny allowlist for every class Avro resolves out of a schema: sixteen `java.lang`
  and `java.math` names are trusted, `org.apache.avro.SERIALIZABLE_PACKAGES` has no default,
  and everything else raises `SecurityException`. That is 130 failing tests here, all of them
  `SpecificRecord` paths, and it is green again the moment the property is set — so it is not a
  fork defect but a contract change Avro pushes onto every consuming application. Camel and
  Pulsar are reporting the same. Taking it means telling every consumer of these artifacts to
  set a system property before their next deploy, which is a migration to schedule rather than
  a patch to absorb, so `dependabot.yml` holds the group at `>= 1.12.2` until then.

- **`org.apache.avro.Schemas` removed.** `avro-kafkaconnect-converter` carried
  `src/main/kotlin/org/apache/avro/Schemas.kt`, Confluent's helper as Amazon inherited it:
  a class placed inside Avro's own package so it could reach `Schema.FACTORY`,
  `Schema.Names` and the package-private `schema.toJson(names, gen)`. Its whole body is a
  transcription of `Schema.toString(Collection<Schema>, boolean)`, which has been **public**
  since long before either fork — the hack was never needed. `AvroSchema.canonicalString`
  now calls that method directly and the file is gone, which also stops the build publishing
  a class into a package it does not own. The method carries `@Deprecated` from 1.12 on
  ("should be removed with AVRO-2832"); depending on a deprecated public method is a
  position the fork can hold, reaching into package-private internals was not.
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
- **The four shaded modules publish an ordinary jar, not an uber-jar.** `maven-shade-plugin`
  was configured with an empty `<configuration/>`, so `schema-registry-serde-msk-iam` and the
  three Connect converters replaced their main artifact with a jar holding their whole runtime
  classpath, published a pom stripped of every dependency bar `slf4j-api`, and disabled the
  Gradle module metadata. They now publish like every other module — a thin jar, a complete
  pom, module metadata — and the self-contained bundle survives as a **plugin distribution**: a
  zip of the jar and its runtime classpath, laid out as a Connect plugin directory, attached to
  each GitHub Release instead of being pushed to a Maven repository. A publication goes from
  284 MB to 7.9 MB. What was in those jars, and why the bundle moved rather than shrank, is in
  [build.md](build.md#the-plugin-distributions).
- **The `tests` classifier of `schema-registry-serde` is built but no longer published.**
  The Maven build ran `maven-jar-plugin:test-jar` and the port carried the artifact into the
  publication. What needs it is `integration-tests`, which reuses `ProtobufGenerator` and the
  classes generated from `src/test/proto` — and it reaches them through the `testArtifacts`
  project configuration, not through a repository. Published, the artifact was unusable
  anyway: a `tests` classifier inherits the main pom, which declares neither JUnit 5 nor
  Mockito while 322 and 234 of the jar's classes reference them, so resolving it gave a
  `NoClassDefFoundError` until the consumer rebuilt the test classpath by hand. Dropping it
  takes a release from 231 files to 226, against the 1,167 a month Maven Central meters on the
  free tier.
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
  now land in `META-INF/` of every artifact, and beside the `lib/` directory of every plugin
  distribution.
- **`NOTICE.txt` states the modifications.** ALv2 §4.b requires a modified distribution
  to carry prominent notice of the change. The upstream `NOTICE.txt` held the Amazon
  copyright line alone; the fork's modifications are now listed after it, along with the
  disclaimer that the fork is not endorsed by Amazon.
- **`THIRD-PARTY-LICENSES.txt` generated instead of versioned.** The inherited file
  described the dependency graph of the Maven build, a set this fork no longer has: the
  C# modules are gone, Kotlin, Wire and `at.yawk.lz4` came in. It is replaced by a
  `thirdPartyLicenses` task, built on `com.github.jk1.dependency-license-report`, which
  derives the inventory from the resolved `runtimeClasspath` and packages it into the four
  plugin distributions — the only artifacts that physically carry third-party code. Generating
  it per build is what keeps it from drifting a second time. The report's generation timestamp
  is stripped so that two builds of the same commit produce identical archives.
- **`slf4j-api` is declared, never bundled into a classpath a consumer cannot arbitrate.**
  While the four modules shipped uber-jars, embedding it was a trap for a standalone
  application: `org.slf4j.LoggerFactory` 2.x binds through an `SLF4JServiceProvider`, and a
  classpath that served the uber-jar's copy first handed a 1.x stack — logback 1.2,
  `log4j-slf4j-impl` — a `LoggerFactory` that finds no provider, which turns every logger in
  the process into a NOP, not just this library's. The jar excluded it and the pom declared it.
  Now that those modules publish a normal pom, the exclusion has no reason to exist: a build
  tool resolves one `slf4j-api` and the consumer's own binding wins. The plugin distributions
  do carry the jar, and Kafka Connect is immune by construction — its plugin classloader
  parent-delegates `org.slf4j`, so the worker's own copy is the one that loads.
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
- **Dependency scopes narrowed to what each module actually exposes.** The pom put every
  dependency at `compile` scope, and the Gradle port carried that over as `api` on all ten
  modules. `api` propagates to a consumer's _compile_ classpath, so consumers were compiling
  against the whole transitive world of the fork — including, for `schema-registry-serde`,
  the embedded Kotlin compiler (`kotlin-scripting-compiler-embeddable` and its `-impl-`
  twin, some 55 MB), `kotlin-reflect`, `wire-compiler` and `okio-fakefilesystem`. A
  dependency now stays `api` only when a type from it appears in that module's committed
  `.api` dump; everything else is `implementation`, and what no source references at all is
  `runtimeOnly`. Nothing was added to or removed from any published pom: every entry that
  was at `compile` is still there, at `runtime`. The three dependencies `common` stopped
  exposing but `schema-registry-serde` genuinely exposes — `avro`, `guava`, `commons-lang3` —
  are declared on `schema-registry-serde` instead, so a consumer of that module sees exactly
  the compile classpath it saw before. A consumer of `schema-registry-common` on its own
  does lose them at compile time, which is the point of the change. The eight `.api` dumps
  are byte-identical before and after.
- **Jackson is declared, no longer inherited.** `jackson-databind` was used directly by
  `common`, `serializer-deserializer`, `avro-kafkaconnect-converter` and
  `jsonschema-kafkaconnect-converter` while being declared by none of them: it arrived
  through `avro` (2.14.3), `connect-json` (2.16.2) and `aws-msk-iam-auth` (2.18.3), so the
  version a module compiled against was whatever conflict resolution happened to pick, and
  it differed between modules. Every module that imports `com.fasterxml.jackson` now
  declares the artifacts it uses on top of `platform(libs.jackson.bom)`, which aligns the
  whole graph on the 2.22.2 the catalog had been pinning for `examples` alone. The scope
  follows the rule above — `api` for `serializer-deserializer` and
  `jsonschema-kafkaconnect-converter`, whose dumps carry `JsonNode`, `implementation` for
  `avro-kafkaconnect-converter`, whose Jackson use is entirely private — with one exception:
  `common` keeps it at `api` although no Jackson type appears in its dump, because
  `GlueSchemaRegistryConfiguration` exposes
  `List<SerializationFeature>`/`List<DeserializationFeature>` and a dump written in JVM
  descriptors erases the type argument. The dump decides for a bare type, not for one that
  only ever appears inside a generic.
- **A truncated compressed payload raises instead of spinning forever.**
  `GlueSchemaRegistryCompressionHandler.writeToByteArrayOutputStream` looped on
  `while (!inflater.finished())`. On a zlib stream that ends mid-data — which the producer
  of a record controls — `Inflater.inflate` returns 0, `needsInput()` becomes true and
  `finished()` never does, so the loop spins on a CPU until the process is killed. The loop
  now exits when `inflate` produced nothing and the inflater is waiting on more input,
  raising an `AWSSchemaRegistryException` naming the truncation. A stream waiting on a
  preset dictionary hangs the same loop for a different reason, and raises its own message
  saying so. Every
  well-formed payload decompresses exactly as before; a caller that was hanging now gets the
  same `AWSSchemaRegistryException("Error while decompressing data")` it gets for any other
  malformed input, with the truncation as its cause.
- **Configuration errors name the property instead of leaking a cast.**
  `GlueSchemaRegistryConfiguration` read half its properties through a raw `as String`, so a
  caller passing `200` rather than `"200"` for `cacheSize` — an entirely reasonable thing to
  do with a `Map<String, Object>` — got a bare `ClassCastException` naming neither the
  property nor the expected type. Those casts now raise an `AWSSchemaRegistryException`
  naming the property and the type that was supplied. The value is still rejected: only the
  exception type and the message change. The same applies to the entries of the two Jackson
  feature lists. Separately, the "Invalid Compression type" message interpolated the result
  of `COMPRESSION.values()`, printing `[Lcom.amazonaws…;@1b6d3586` where the accepted values
  were meant to be; it now reads `NONE, ZLIB`.
- **The schema-evolution poll waits between attempts.** `waitForSchemaEvolutionCheckToComplete`
  slept three seconds once, then issued its ten `GetSchemaVersion` calls back to back with
  nothing between them. Ten calls fired inside a few hundred milliseconds is both a burst
  against a throttled API and a retry loop that gives the check no more time to finish than
  a single attempt would have. The gap between attempts is now exponential, from 100 ms and
  doubling up to the same three-second `MAX_WAIT_INTERVAL` the initial sleep already used,
  so the ten attempts span about eighteen seconds instead of three. Nothing else changes:
  the attempt count, the statuses accepted, and the two exceptions raised are the same. A
  caller that registers a schema whose evolution check is slow now blocks longer — and
  succeeds where it used to exhaust its retries.
- **`ProtoFileElement` built with named arguments.** Wire 6 inserted a `weakImports`
  parameter between `publicImports` and `types`. The call in `FileDescriptorUtils` passed
  its nine arguments positionally, so the insertion shifted every argument after the fifth
  onto the wrong parameter — caught by the compiler here only because the types differ.
  The call now names its arguments and lets `weakImports` take its default, so the next
  parameter Wire inserts cannot silently land in the wrong slot.
- **Widened visibility on a few nested types.** `ProtobufSchemaLoaderContext` was
  `protected static` and `AvroData.FromConnectContext` was `private static`, both exposed
  through public methods — legal in Java, rejected by Kotlin. They are now public classes
  with an `internal` constructor, so they still cannot be built from outside the library.
- **POJO classes are resolved through the thread context class loader.** `Class.forName(name)`
  resolves through the class loader that defined the calling class — this library's. When the
  application is loaded apart from its dependencies, which is the normal arrangement for a Kafka
  Connect plugin directory, a repackaged Spring Boot jar or an application server, that loader
  cannot see the application's own classes, so `protobufMessageType=POJO` and JSON `className`
  resolution both fail with a `ClassNotFoundException` naming a class that is demonstrably on the
  classpath. `PojoClassResolver` asks the thread context class loader first and falls back to
  `Class.forName`, so the resolution can only find more classes than before, never fewer. This is
  upstream PR #336, extended to the JSON POJO path, which has the same defect for the same reason.
  The two other reflective lookups in the library are deliberately left alone:
  `SecondaryDeserializer.validate` and `GlueSchemaRegistryUtils.initializeStrategy` compare what
  they load against a type this library loaded itself, and resolving one side through a different
  class loader would make that comparison fail rather than succeed.
- **The Kafka Connect converters describe their configuration.** The three converters built
  their `AbstractConfig` from an empty `ConfigDef()`, so `Converter.config()` returned the
  empty default of the interface: `PUT /connector-plugins/{plugin}/config/validate` reported
  nothing about the registry settings, a Connect UI had nothing to render, and a misspelled
  key or an impossible value was discovered at the first record rather than when the
  connector was created. Every key
  `GlueSchemaRegistryConfiguration` reads is now declared — type, default, accepted values,
  importance, group and documentation — in `GlueSchemaRegistryConfigDef`, which the three
  converter config classes assemble and which each converter now returns from `config()`.
  What this buys is value validation, not key validation: `AbstractConfig` parses the keys it
  knows and passes the rest through untouched, so a misspelled key is still ignored in silence.
  That is also what lets `tags` and `metadata` be left undeclared — their values are maps and
  Kafka's `ConfigDef` has no map type, so declaring them would reject a configuration that works
  today. `userAgentApp` is left undeclared for a different reason: each converter sets that field
  on its serializer and deserializer, whose `configure` then overwrites the configuration value
  with it, so the key has no effect through a converter and documenting it would be a lie. And the values of the declared keys go
  through `GlueSchemaRegistryConfigDef.coerce` first, which renders a non-`String` value of a
  `STRING` key through `toString()`, and a `Class` through `getName()`, because that is what
  `GlueSchemaRegistryConfiguration` itself accepts for those keys and `ConfigDef` accepts
  neither. `coerce` also splits a comma-separated value of a `LIST` key, and the converters hand
  the rendered map on to their serializer and deserializer rather than the one they were given:
  a Connect worker can only supply strings, so `jacksonSerializationFeatures` was unusable from a
  worker before — the value reached `GlueSchemaRegistryConfiguration` as a `String` and was
  rejected as "should be a list". When nothing needs rendering, `coerce` returns the very map it
  was given. What the converters reject that they used to ignore is therefore only what the
  registry configuration would have rejected later anyway. `ProtobufSchemaConverter`, which
  had no config class at all, gained `ProtobufSchemaConverterConfig` and now validates its
  configuration like the other two. The enum validators follow the parsing the registry
  configuration performs: case-insensitive for `compression`, `compatibility` and
  `dataFormat`, which are uppercased before parsing, case-sensitive for `avroRecordType` and
  `protobufMessageType`, which reach `Enum.valueOf` as they are given.
- **A nullable union is optional however JSON Schema spells it.**
  `JsonSchemaToConnectSchemaConverter` recognised "one real type plus null" only when everit
  had modelled it as `oneOf` with exactly two subschemas. A `"type": ["string", "null"]` array
  is parsed by everit as `anyOf`, so it fell through to the non-optional union path, which
  calls `optional()` on the builder, and then to `populateConnectProperties`, which calls
  `required()` on the same builder — Connect's `SchemaBuilder` rejects that with
  `Invalid SchemaBuilder call: optional has already been set`. The commonest way of writing a
  nullable field in JSON Schema was therefore unreadable by the Connect converter. Any
  `oneOf` or `anyOf` containing a `NullSchema` is now treated as nullable, and the resulting
  schema is the union of the remaining types, made optional: one real type yields that type
  directly, several yield the `oneOf` struct over them. The `oneOf`-with-two-subschemas case
  behaves exactly as before; every other shape used to raise. This is upstream PR #526
  (upstream issue #218). Two details beyond it: a union of nothing but `null` yields no schema,
  as a bare `NullSchema` already does, rather than an empty `oneOf` struct; and the branches of
  a nullable multi-type union are optional, so a value that populates one branch validates. That
  last point does not extend to a **non**-nullable multi-type union, whose branches are still
  required and which therefore still cannot carry a value — a pre-existing limitation of the
  union encoding, untouched here because changing it would move schemas that convert today.
- **The JSON Schema converter caches key on everything that shapes the result.** Both caches
  were keyed on the schema alone although the conversion also depends on the other arguments,
  and everit and Connect schemas compare by value, so two conversions of equal schemas
  collided. `toConnectSchema(schema, required)` bakes `required` into the Connect schema it
  builds: in `{"a": {"type": "string"}, "b": {"type": ["string", "null"]}}` the field `a` is
  converted with `required = true` and the union branch of `b` with `required = false`, so
  whichever ran first decided both — `b` came out required, or, with the properties the other
  way round, `a` came out optional. A record with `b` null then failed
  `ConnectSchema.validateValue`, and a JDBC sink created the column `NOT NULL`. The mirror
  `fromConnectSchema(schema, ignoreOptional, index)` writes `index` into `connect.index`, so a
  struct with two fields of the same Connect type gave both of them the first one's index and
  lost the ordering that property exists to carry. The keys are now `Pair(schema, required)`
  and `Triple(schema, ignoreOptional, index)`. The defect is upstream's, and upstream cannot
  reach the `toConnect` half of it: before the entry above, no nullable union converted at all,
  so `required` never varied. The caches stay public `val`s and their erased signatures are
  unchanged, so the ABI dumps do not move; the entry counts asserted by the existing
  cache-size tests do not move either, since each of their calls uses one argument value.
- **Nullable fields can be opted into when a JSON schema is derived from a POJO.**
  `JsonSerializer` built its `JsonSchemaGenerator` with the mbknor default, which types an
  optional field as its type alone. A POJO whose optional field is null therefore serialises
  to `"field": null` and then fails its own generated schema — the object cannot be sent at
  all. The new `jsonSchemaNullableEnabled` property switches the generator to
  `JsonSchemaConfig.nullableJsonSchemaDraft4()`, which emits `oneOf [null, type]` for those
  fields. It defaults to false because turning it on changes the schema text, and therefore
  registers a new schema version. This is upstream PR #529 (upstream issue #73).
- **JSON schema compatibility can be checked on the client.** Glue enforces the compatibility
  mode of a schema for Avro and Protobuf but not for JSON, so a JSON schema version that breaks
  its declared mode is accepted by `RegisterSchemaVersion` and the breakage surfaces in a
  consumer instead of in the producer that caused it. The new
  `jsonSchemaCompatibilityCheckEnabled` property makes `AWSSchemaRegistryClient` read the latest
  version of the schema before registering a new one and compare the two through
  `JsonSchemaCompatibilityChecker`. What is compared is the `required` contract, at the top level
  and inside each named entry of `definitions` or `$defs`; nothing else a schema can say is
  compared, so a clean result means "no broken `required` contract" rather than "compatible".
  This is upstream PR #494, with two deliberate changes. It is **opt-in**: upstream enabled it
  whenever the compatibility mode was neither absent nor `NONE`, which is every JSON producer,
  since the default mode is `BACKWARD` — an incomplete check that can refuse a registration is
  not something to switch on under everyone by default, and it costs one extra `GetSchemaVersion`
  call per newly registered schema definition. And the modes are read from the enum rather than
  from the prefix of its name, so `DISABLED` disables the check as `NONE` does; upstream's
  string test let `DISABLED` through.
- **A Kotlin module was added.** `serde-kotlin` (`com.mobsuccess:schema-registry-serde-kotlin`)
  is the first module that is not carried over from the source repository, and the first whose
  package is `com.mobsuccess` rather than `com.amazonaws.services.schemaregistry`: it is the
  fork's own API, and putting it under Amazon's package would say otherwise. It is strictly
  additive — nothing in the other modules refers to it, and it introduces no behaviour, only a
  configuration DSL that produces the same property map the Java API takes and a `Serde<T>`
  wrapper over `GlueSchemaRegistryKafkaStreamsSerde`. The wrapper checks the type of each record
  rather than casting blind, so a topic that does not hold what the caller declared fails where
  the record is read instead of somewhere further down a Kafka Streams topology.
- **Two hand-written lazy initialisations became `by lazy`.** `ProtobufSchema.getProtobufFile`
  read a field, built the value if it was null and assigned it back, with no synchronisation at
  all: two threads calling it at once could each build a `ProtobufFile` and see different
  instances, and the assignment was not safely published. `GlueSchemaRegistryCompressionFactory`
  had the same shape guarded by `@Synchronized`, which was correct but took a monitor on a path
  a deserializer walks per record. Both are now `by lazy`, whose default mode is synchronized and
  publishes safely, and which takes no lock once the value is built. `ProtobufSchema` is not
  reachable from the library today — nothing constructs it — but it is published API, so the race
  was real for a caller who used it.
- **The three hand-written singletons became `object`s.** `GlueSchemaRegistryUtils`, `AVROUtils`
  and `GlueSchemaRegistryDeserializerDataParser` were each a class with a private constructor, a
  companion holding an `INSTANCE`, and a `getInstance()` returning it — which is what a Kotlin
  `object` is. `getInstance()` is kept as a `@JvmStatic` shim, so every caller, Java or Kotlin,
  is unaffected. The `.api` dumps lose the three `$Companion` classes and their `Companion`
  fields and gain the `INSTANCE` field an `object` declares; the static `getInstance()` that
  callers actually use does not move.

  That `Companion` was **not** part of the surface this fork promises to preserve. The Java
  source used the initialization-on-demand holder idiom — a _private_ nested `UtilsHelper` /
  `DataParserHelper` holding the instance, reached through a public static `getInstance()` — so
  the only public member was `getInstance()` itself. The conversion to Kotlin introduced
  `Companion` as a public member the Java original never had; removing it narrows the surface
  back towards the source rather than away from it. The `INSTANCE` field an `object` declares is
  new for the same reason the `Companion` was, and is equally not something a caller needs.
  Laziness is unchanged in all three cases: the holder class, the companion and the `object` are
  each initialised on first touch, which is the first `getInstance()` call.

- **`mbknor-jackson-jsonschema` taken from its Scala 2.13 build.** The pom depended on the
  `_2.12` artifact, whose `scala.Serializable` supertype no longer exists in Scala 2.13. A
  consumer whose platform pins `scala-library` at 2.13 — the Quarkus BOM does — therefore
  resolved a classpath on which `JsonSchemaGenerator` cannot link, and a GraalVM native
  image built from it died on an unresolved `JsonSchemaConfig$` type. The `_2.13` artifact
  is the same 1.0.39 release cross-compiled, so the JSON Schema support is unchanged; only
  the Scala runtime the consumer ends up with is.
- **GraalVM reachability metadata packaged in the jars.**
  `schema-registry-serde` and `schema-registry-kafkastreams-serde` ship a
  `META-INF/native-image/com.mobsuccess/<artifactId>/` directory, read automatically by
  `native-image` from the classpath. It registers the two things a native consumer cannot
  work out for itself: the entry points named by class name in a Kafka or Kafka Streams
  configuration, which nothing references statically — the four `*KafkaSerializer` /
  `*KafkaDeserializer` classes and the two `Serde` ones — and the 29 `.proto` files
  `ProtobufSchemaLoader` reads off the classpath through `getResourceAsStream`, which are
  not embedded in an image unless declared and whose absence breaks the Protobuf path at
  run time rather than at build time. `com.google.protobuf.DescriptorProtos` is registered
  for reflection alongside them, for the descriptor parsing that path performs. This
  packages, once and for every consumer, configuration each of them was otherwise writing
  by hand.
- **Using a serializer, deserializer or converter before `configure()` now says so.** Every one
  of these types keeps its collaborators in nullable properties that `configure()` fills, and
  every use of them was written `field!!`. Calling `serialize`, `deserialize` or a converter
  before `configure` therefore raised a `KotlinNullPointerException` with **no message**, from a
  line that named a field the caller has never heard of — the least diagnosable failure the
  library could produce, on the one mistake a Kafka integration makes most often. Each public
  operation now binds its collaborators once, through
  `checkNotNull(field) { "configure() has not been called…" }`, and the local it binds is what
  the body uses. The declarations do not move: the properties inherited from Lombok `@Setter`
  stay public, mutable and nullable, so nothing on the published surface changes.

  This changes the exception from `NullPointerException` to `IllegalStateException`, which is
  the one deliberate behaviour change. It is the right type — the object is being used in a
  state it is not in, which is what `IllegalStateException` means, and what Kafka itself raises
  for lifecycle misuse — and no message is lost, since there was none. One inherited test
  asserted the old type: `testPrepareInput_nullDefinitionData_throwsException` on the generic
  serializer, which despite its name never reached the data at all — `prepareInput` dereferenced
  the unconfigured facade on its first line, so what the test caught was the missing `configure`.
  It now asserts the state exception and the message, under a name that says which of the two it
  checks. The `!!` on values a caller passes in — a null `credentialProvider`, a null record — is
  left alone: those are argument errors, not lifecycle errors, and they keep raising
  `NullPointerException` as the Java source did.

- **The `@Data` classes lost `equals`, `hashCode`, `toString` and some setters.** Six types
  carried Lombok `@Data` upstream — `JsonSchemaConverter`, `ConnectSchemaToJsonSchemaConverter`,
  `JsonSchemaToConnectSchemaConverter`, `AWSKafkaAvroConverter`,
  `GlueSchemaRegistryKafkaSerializer` and `AWSKafkaAvroSerializer` — so every field of theirs had
  a public getter _and_ a public setter, and each class had value `equals`, `hashCode` and
  `toString`. The conversion kept the getters and the setters a caller plausibly uses, and
  dropped the rest: the setters for collaborators supplied through the constructor
  (`setSerializer`, `setDeserializer`, `setCredentialProvider`, `setSchemaVersionId`,
  `setObjectMapper`, the `TypeConverterFactory` and `JsonSchemaDataConfig` accessors) and the
  three `Object` overrides.

  That is not restored, deliberately. `equals` and `hashCode` over the mutable collaborators of a
  stateful converter are not a value contract anyone can rely on: `configure()` replaces those
  collaborators, so a converter added to a `HashSet` beforehand hashes to one bucket and is then
  looked for in another, and the set can neither find it nor report it as present. Swapping a
  serializer out from under a running converter is not an operation this library should keep
  offering either. The narrowing is real, and is recorded here rather than reverted. `Schema`,
  the one `@Value` type among them, is a Kotlin `data class` and keeps all three overrides.

  The audit that prompted this entry also reported the opposite defect — that the JSON Schema
  converter caches, described there as private in Java, had become public `val`s by accident.
  They had not: `ConnectSchemaToJsonSchemaConverter` and `JsonSchemaToConnectSchemaConverter` carry
  `@Data`, so `getFromConnectSchemaCache()` and `getToConnectSchemaCache()` were already public,
  with setters. Making them `internal` would have narrowed the surface away from the source, not
  towards it. A sweep of every converted class for a field that is public in Kotlin and had no
  public accessor in Java — Lombok annotations accounted for — finds **no** accidental widening
  anywhere in the port.

- **Duplication removed where it was literal.** `AWSSchemaRegistryClient` carried two private
  functions with different names and identical bodies, `getSchemaVersionRequest` and
  `getGetSchemaVersionRequest`; one is gone. `SecondaryDeserializer` had four methods each
  catching three reflective exceptions in three blocks with the same body — twelve blocks
  producing four distinct messages. Each is now one `catch (e: ReflectiveOperationException)`,
  which is the common supertype of all six exception types involved and is not wider in
  practice: `Class.forName`, `newInstance`, `getMethod` and `invoke` throw no other subclass of
  it, so nothing newly falls into these handlers.
- **The two single-method validation interfaces became `fun interface`s.** `SchemaValidator` and
  `SchemaValidationStrategy` were implemented five times in `SchemaValidatorBuilder` by anonymous
  objects; they are lambdas now, and the file is a third of its length. The strategy is still
  read from the field at validation time rather than captured when the validator is built, so a
  builder reconfigured after handing out a validator still changes what that validator does — the
  inherited behaviour, kept deliberately. `fun interface` changes no signature, so the `.api`
  dump does not move.

  None of this cluster — `SchemaValidator`, `SchemaValidationStrategy`, `SchemaValidatorBuilder`,
  `CompatibilityChecker` — is reached from anywhere in the library, and none of it had a test.
  `SchemaValidatorBuilderTest` was written first against the inherited implementation, and pins
  what the rewrite had to preserve: which of the two schemas each strategy interrogates, that
  `validateLatest` reads only the first, and that `validateAll` stops at the first schema to
  report something rather than visiting the rest.

- **`GlueSchemaRegistryKinesisIntegrationTest` is no longer byte-identical to upstream.**
  Until the Kotlin conversion, that file differed from `awslabs`' only by the STS redirect
  that points the KPL child process at LocalStack, so `git diff eed1506 -- <path>` read as a
  four-line patch. In Kotlin it is a rewrite, and upstream changes to it now have to be
  merged by hand rather than applied. The redirect itself — `setStsEndpoint` /
  `setStsPort` — survives, and must: without it the native producer resolves the stream ARN
  through the real AWS STS, which rejects the emulator's dummy credentials and takes the
  child process down with an opaque `DaemonException: The child process has been shutdown`,
  failing all 15 KPL tests at once.

- **Lombok is gone.** `integration-tests` was the last module using it: `@Slf4j` (3),
  `@Getter` (3), `@Builder` (3), `@AllArgsConstructor` (2), `@EqualsAndHashCode` (1) and
  `@SneakyThrows` (1). The loggers became `LoggerFactory.getLogger` in a companion object,
  the getters Kotlin `val`s, and each `@Builder` a hand-written nested `Builder` with a
  `@JvmStatic builder()`. `Car.equals` reproduces `@EqualsAndHashCode`'s deep array
  comparison by hand — its `String[] owners` is compared by the JSON specific-record tests,
  which an identity comparison would turn red. The dependency, the root `lombok.config` and
  the `libs.versions.toml` entry are all deleted.

- **The static constants of `GlueSchemaRegistryConnectionProperties` moved to a companion
  object.** In Java it was an interface holding `REGION` and `ENDPOINT`, and `KafkaHelper`
  read them off an instance — `consumerProperties.ENDPOINT` — which Java permits for an
  inherited static field and Kotlin does not. The call sites now name the interface. The
  values, and the `GLUE_ENDPOINT` override this fork added, are unchanged.

- **Two schema paths corrected in `AvroGenericBackwardAllCompatDataGenerator`.** Upstream
  reads `backwardAll2.avsc` and `backwardAll3.avsc` from `src/test/resources/backwardAll/`,
  a directory that does not exist — the files are under `avro/backwardAll/`, where the same
  method reads `backwardAll1.avsc` from. This is the one place the conversion changed what
  the code does rather than how it is written, and it is observable by nothing:
  `TestDataGeneratorFactory` has no `AVRO_GENERIC_BACKWARD_ALL` case, so it throws before
  the generator can be constructed, and no test asks for that combination. The typo is
  corrected rather than carried forward so that wiring the factory up is a one-line change
  instead of a debugging session.

- **protobuf 4, and the Kinesis libraries that come with it.** The pom pins
  `protobuf.version` at 3.25.5 and, directly above the KPL version, carries the comment
  _"LATEST KPL will cause integration test failure in Linux environment, update once we
  find a way to address the issue"_ — with a second one above the KCL version, _"LATEST KCL
  Does not work with LocalStack yet, remove once new version works"_. Upstream still holds
  all three pins. This fork moves off them together:

  |                                                             | Original pom                                   | Here                                                    |
  | ----------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------- |
  | `com.google.protobuf:protobuf-java`, `-java-util`, `protoc` | 3.25.5                                         | 4.36.0                                                  |
  | Kinesis Producer Library                                    | `com.amazonaws:amazon-kinesis-producer:0.15.8` | `software.amazon.kinesis:amazon-kinesis-producer:1.0.7` |
  | `software.amazon.kinesis:amazon-kinesis-client`             | 2.2.9                                          | 3.5.1                                                   |
  | `io.apicurio:apicurio-registry-protobuf-schema-utilities`   | 2.1.3.Final                                    | 3.3.1                                                   |

  They are one change, not four. The KPL is the only artifact in the build that carries a
  protobuf version of its own — it ships generated code — and 1.0.7 generates against 4.29.0.
  On 3.25.5 that mismatch is fatal, which is what #115 diagnosed: Gradle resolves the highest
  requested version, protobuf 4 displaced the 3.25.5 everything else was compiled against, and
  every protobuf path died on `NoSuchMethodError: DescriptorProtos$FieldOptions.hasExtension`.
  #115 restored the old KPL because the alternative was to move protobuf, which is what this
  entry records instead.

  proto3 the language and the wire format are unchanged; no `.proto` file is touched. The
  4.x jump is a cross-language version realignment. What moves is the Java binary interface:
  generated messages now extend `GeneratedMessage` rather than `GeneratedMessageV3`, some
  overloads are gone, and generated code checks `com.google.protobuf.RuntimeVersion` at class
  load. Generated sources must therefore be produced by a matching `protoc`, which is why the
  catalog's `protobuf` version drives `protobuf-java`, `protobuf-java-util` and `protoc`
  alike. Recompiling is enough; the source-level API is compatible bar the one method below.

  `protobuf-java` is `api` on `schema-registry-serde` and is used by fourteen of its main
  sources, so this is breaking for consumers of the published artifacts: they have to move to
  protobuf 4 too. `serializer-deserializer/api/schema-registry-serde.api` records it — the
  dump for `additionalTypes.Decimals` and `metadata.ProtobufSchemaMetadata` changes
  superclass.

  This is a durable divergence from upstream, and the first one the identical-behaviour
  contract does not cover: it is not a conversion artefact but a deliberate step away from a
  version the source repository still pins. It is taken because 3.25 is the last 3.x line
  there is — its final patch, 3.25.8, landed in May 2025, and the build was three behind it
  at 3.25.5 — because the KPL and the KCL cannot be updated without it, and because a fork
  that cannot take a security bump on protobuf has no path forward. The contract that still
  holds is the one that matters: the inherited suite runs green and has not shrunk.

- **Flink 1.12.2 → 1.20.5, the LTS line.** The pom pins `flink.version` at 1.12.2 and takes
  `org.apache.flink:flink-streaming-java_2.11`. That coordinate is a dead end: Flink dropped
  the Scala suffix from `flink-streaming-java` after 1.14, so no later version of it exists,
  and 1.12 last had a patch in 2021. The catalog now pins `flink` at 1.20.5 and takes the
  suffix-free `org.apache.flink:flink-streaming-java`.

  **No source changed.** Every Flink type the module touches is identical between the two
  releases: `SchemaCoder` and `SchemaCoder.SchemaCoderProvider` declare the same two methods,
  `RegistryAvroDeserializationSchema` and `RegistryAvroSerializationSchema` keep the
  `(Class, Schema, SchemaCoderProvider)` constructor the module extends, and
  `MutableByteArrayInputStream` is unchanged. 1.20 only adds `AvroFormatOptions.AvroEncoding`
  overloads alongside them. The one signature that moved — `AvroSerializationSchema.getEncoder()`
  returning `Encoder` rather than `BinaryEncoder` — is called for `flush()` only, which
  `Encoder` declares. `avro-flink-serde/api/schema-registry-flink-serde.api` is unchanged, and
  the module's 21 tests pass untouched. Upstream proposes the same three coordinate lines in
  [awslabs#374](https://github.com/awslabs/aws-glue-schema-registry/pull/374), still open there.

  That one moved signature does set a **runtime floor of Flink 1.19**: `getEncoder()` returns
  `BinaryEncoder` up to 1.18 and `Encoder` from 1.19, so the compiled call site in
  `GlueSchemaRegistryAvroSerializationSchema.serialize` resolves to a method an older runtime
  does not declare.

  The three duplicate `commons-compress` / `lz4-java` exclusion blocks are folded into one
  `Action<ExternalModuleDependency>`. `runtimeClasspath`, `testRuntimeClasspath` and
  `compileClasspath` were diffed before and after: identical. The `commons-compress` half is
  still live — dropping it pulls 1.26.x back in through Avro — while the `org.lz4` half is now
  inert, and is kept so both Flink dependencies carry the same rule.

  Avro does not move with it. `flink-avro` carries a hard `org.apache.avro:avro` dependency,
  and every Flink line through 2.3 pins it at 1.11.4 — the version this build was already on —
  so 1.20 resolves the same Avro as 1.12.2 did.

  Like the protobuf entry above, this is a deliberate step away from a version upstream still
  pins rather than a conversion artefact. It is taken because 1.12.2 is unpatchable and its
  Scala 2.11 coordinate is unbumpable, so a consumer on any supported Flink was already
  overriding both. The contract that matters holds: the inherited suite runs green and has not
  shrunk.

- **`FieldDescriptor.hasOptionalKeyword()` reimplemented.** protobuf 4 made the method
  package-private. `ProtobufDataToConnectDataConverter` and
  `ProtobufSchemaToConnectSchemaConverter` both call it, so
  `protobuf-kafkaconnect-converter` carries an internal extension of the same name that
  reproduces the library's own body through public API — `proto3Optional` from the field's
  descriptor proto, and proto2 read off the file's `syntax` string. That last part is the one
  liberty taken: the library tests `getFile().getEdition() == EDITION_PROTO2`, and `getEdition()`
  is package-private too, but it is itself derived from `syntax` — anything that is neither
  `"proto3"` nor `"editions"` is `EDITION_PROTO2` — so reading `syntax` back is the same
  predicate, not an approximation of it. `hasPresence()` is public but is not the same
  predicate: it also holds for message fields and for oneof members.

- **`truth-proto-extension` moved to 1.4.5.** 1.1.3 calls
  `Descriptors.FileDescriptor.getSyntax()`, removed in protobuf 4, and takes the sixteen
  `FileDescriptorUtilsTest` cases down with a `NoSuchMethodError`. Test scope only.

- **`icu4j` excluded from apicurio.** 3.3.1 declares `com.ibm.icu:icu4j`, which no class of
  its own references, and it landed on the runtime classpath of
  `protobuf-kafkaconnect-converter` — 13.5 MB of the uber-jar that module used to publish, and
  13.5 MB of the plugin distribution it builds today. The apicurio
  classes themselves are unreachable here in any case — the fork vendors its copy of them
  under `com.amazonaws.services.schemaregistry.utils.apicurio` — but the dependency is the
  pom's and is kept.

- **The published AWS serde excluded from the KPL.** `amazon-kinesis-producer:1.0.7` depends
  on `software.amazon.glue:schema-registry-serde` for its Glue Schema Registry integration.
  Those classes have the package names this repository's own do, so leaving the dependency in
  puts two copies of every serde class on the `integration-tests` classpath, one of them
  compiled against protobuf 3.25.5. The module's own project dependencies supply them.

- **`RecordProcessor` made thread-safe.** All three of its fields are written by the KCL
  scheduler thread, from `GlueSchemaRegistryRecordProcessor`, and read by the test thread —
  two flags with no happens-before between the write and the read, and a plain `ArrayList`.
  Upstream got away with it because the test read them once, after a sleep; the barriers below
  poll them, which turns a latent data race into a flaky one. The flags carry `@Volatile` now
  and the list is a `Collections.synchronizedList`, whose `size` and `toArray` both take the
  list's own monitor — the two operations the barrier and the assertion after it use. This is
  the same defect #115 fixed in `doProduceRecordsMultithreaded`, in the other half of this
  fixture.

- **The KCL waits are barriers, not sleeps.** `GlueSchemaRegistryKinesisIntegrationTest`
  slept a fixed fifteen seconds after starting the `Scheduler` and five more after producing.
  KCL 3 takes about a hundred seconds to reach its worker loop against LocalStack — an IMDS
  probe that has to time out, a lease-table GSI to create, a leader election and a lease
  assignment, each on its own interval — so the producer ran before the consumer held a shard
  iterator, and with `InitialPositionInStream.LATEST` the records were gone by the time it
  did. The two sleeps are now `Awaitility` barriers: on `RecordProcessor.creationSuccess`,
  set from the record processor's `initialize`, and on the consumed count reaching the
  produced one. Same assertions, no arbitrary timing.

- **The integration-test schema teardown tolerates a schema that is already gone.** Both
  `@AfterAll` teardowns delete every schema name their class recorded, and a single
  `EntityNotFoundException` aborted the loop: the class was reported as an `executionError`
  even with every assertion passed, and the names queued after the failing one were never
  deleted, leaking them into the registry a later run reuses. Observed on 2026-08-26 running
  the suite against one long-lived `motoserver/moto:5.2.2` shared by both classes — the Kafka
  class passed its 45 tests, deleted its 18 topic-derived names, then 404ed on `Basic`, the
  first name `ProtobufClassName.normalize` derives from a proto file. Running that class alone
  against the same emulator is clean, and CI, which gets a fresh moto container per run, never
  saw it; why moto forgets a schema it registered was not pinned down. Each delete is now
  caught and logged per name, on the reasoning `GlueSchemaRegistryKinesisIntegrationTest.setUp`
  already applies to a `ResourceInUseException` from a retried `CreateStream`: the
  postcondition the teardown wants is that the schema is absent, and the exception is evidence
  that it is. `schemasToCleanUp` became a `LinkedHashSet` at the same time — several tests
  append to it, and a name recorded twice made the second delete fail for exactly this reason.
  Nothing a test asserts changes; only a teardown that used to stop at the first missing schema
  now finishes.

- **Parameterized test names no longer carry an identity hash or a random UUID.** JUnit
  builds the display name of a `@ParameterizedTest` from `toString()` of each argument, and
  Gradle writes that name — with no method name next to it — into the `name` attribute of
  the JUnit XML. Four classes fed it something that changes on every run:
  `FileDescriptorUtilsTest` and `MessageIndexFinderTest` pass protobuf `Descriptors`, whose
  inherited `toString()` ends in an identity hash; `ProtobufSchemaConverterTest` passes a
  Connect `Struct` whose `AllTypes` row holds a `byte[]`, rendered as `[B@1b6d3586`; and
  `GlueSchemaRegistryDeserializationFacadeTest` passes a `UUID.randomUUID()` schema version
  id. `EnricoMi/publish-unit-test-result-action` diffs those names between runs, so every
  pull request reported hundreds of tests removed and as many added while the run count was
  unchanged — noise over the one signal that would show a class dropping out of the suite,
  which is what the golden rule leans on. The arguments are now wrapped in `Named.of(...)`:
  the descriptors take their `fullName` or file name, the `Struct` column of both converter
  providers the name of its test case, the version id a fixed label. `Named` is unwrapped
  before the argument reaches the test method, so this changes the report and nothing else —
  the ids stay random, and the per-class inventory is the same 75 classes and 2240 tests
  before and after. Found by diffing the `name` attributes of two consecutive
  `./gradlew cleanTest test --no-build-cache` runs, which is also how the fix was verified;
  no other class in the suite differs between two runs. The five methods sharing
  `testDataAndSchemaProvider` now report identical names for a given row, where the random
  ids used to tell them apart — the same duplication `GlueSchemaRegistrySerializationFacadeTest`
  already has from its fixed `SCHEMA_VERSION_ID_FOR_TESTING`, and the reason its rows do not
  need a wrapper.

- **`KafkaHelper` closes the producers it creates.** Three of its helpers built a
  `KafkaProducer` and dropped it on the floor: `doProduceRecords` and
  `doProduceAvroRecordsSerde` one per call, `doProduceRecordsMultithreaded` four — one per
  thread, every call. Each leak holds a `kafka-producer-network-thread`, a 32 MiB
  `buffer.memory` allocation and a metrics registry until the JVM exits, so a full run of
  `GlueSchemaRegistryKafkaIntegrationTest` accumulated the producers of 12 multithreaded
  cases on top of the single-producer ones. The leak is upstream's, inherited verbatim, and
  was reported by Copilot on the conversion pull request (#134); it was left out of that one
  deliberately, since the conversion was measured as behaviour-preserving against a
  before/after test inventory and a resource-lifetime change is not that. The three are now
  `use { }` blocks. Closing is safe at exactly the point they return because `produceRecords`
  ends on `producer.flush()`: every record is delivered before `close()` can run, and the
  list of `ProducerRecord`s it builds is the block's value, so no caller sees a different
  result. In the multithreaded path the construction moved inside the existing `try`, which
  also means a producer that fails to build now surfaces as the `CompletionException` that
  block already wraps every other failure in, rather than escaping `runAsync` raw — the
  future fails either way. Nothing a test asserts changes; the suite is the same 73 tests,
  none failing.

- **The Avro Connect converter no longer builds an STS client for the region `"null"`.**
  `AWSKafkaAvroConverter.configure` read the region of the assumed-role STS client as
  `configs.get(AWSSchemaRegistryConstants.AWS_REGION).toString()`. That key is optional — the
  rest of the library falls back to the default AWS region provider chain — so in Java the line
  raised a `NullPointerException` from `configure()` whenever `assumeRoleArn` was set without it.
  Kotlin's `Any?.toString()` renders a null receiver as the **string** `"null"`, so the port
  turned that immediate failure into `Region.of("null")`: an STS client that builds happily and
  fails on its first `AssumeRole` call, in a worker that has been up for a while, with an error
  that names no configuration key. The region is now resolved the way
  `GlueSchemaRegistryConfiguration` resolves it for the Glue client — the configured value,
  otherwise the default provider chain — and an `AWSSchemaRegistryException` naming both keys is
  raised from `configure()` when neither yields one. The deviation from the Java source is the
  exception: an absent region with an `assumeRoleArn` raised an unmessaged `NullPointerException`
  there, and either resolves through the provider chain here — the behaviour every other consumer
  of that key already has — or raises a named `AWSSchemaRegistryException`. `assumeRoleArn` is
  read by the Avro converter alone; the upstream TODO next to it, "add this feature to all other
  converters", is still open, so the JSON Schema and Protobuf converters have no such line to
  fix, and this change does not add the feature to them.
- **`integration-tests/docker-compose.yml` mirrors the CI service set.** The inherited file
  described the stack the Maven build used in 2021: `public.ecr.aws/bitnami/zookeeper` and
  `public.ecr.aws/bitnami/kafka:2.8`, images Bitnami withdrew from its public catalogue, so
  `docker compose up` failed on the pull. It also carried a `zookeeper` port mapping of
  `2181:2182`, which had never pointed at the ZooKeeper client port. The fork's own
  integration stack has meanwhile been written down twice — once in
  `.github/workflows/integration.yml`, once as `docker run` lines in
  [build.md](build.md#running-the-suite-locally) — and the compose file matched neither. It is
  now the workflow's three services and nothing else: `apache/kafka:3.9.1` in KRaft mode (so
  no ZooKeeper at all), `localstack/localstack:3.8` with `sts` in `SERVICES`, and
  `motoserver/moto:5.2.2` for the Glue calls, with the same health checks. The obsolete
  `version:` key and the `links:` block go with them — Compose v2 ignores the first and
  supersedes the second. This adopts the intent of
  [awslabs/aws-glue-schema-registry#534](https://github.com/awslabs/aws-glue-schema-registry/pull/534)
  by [@subramp](https://github.com/subramp), which fixes the same broken images by moving to
  the Confluent ZooKeeper and Kafka images; the fork follows its own CI instead, so that a
  local failure and a nightly failure mean the same thing.
- **A missing `SPECIFIC_RECORD` class is named instead of failing on a cast.**
  `DatumReaderInstance.from` resolved the reader class with
  `SpecificData.get().getClass(writerSchema) as Class<SpecificRecord>`. `getClass` returns
  `null` when the class generated from the schema is not on the classpath — the whole
  premise of `SPECIFIC_RECORD` — and the Java source then threw a `NullPointerException`
  from the implicit cast, which the Kotlin port turned into
  `null cannot be cast to non-null type java.lang.Class<…SpecificRecord>`. Either way the
  message names neither the schema nor the missing class, and it arrives wrapped in the
  `AWSSchemaRegistryException` of `AvroDeserializer.deserialize`, two levels below the
  configuration that caused it. The null is now checked and raises an
  `AWSSchemaRegistryException` naming the schema's full name, the classpath requirement and
  `avroRecordType=GENERIC_RECORD` as the way out. Only the exception type and the message
  change: the same schema is rejected at the same point. This closes the remaining half of
  [awslabs/aws-glue-schema-registry#101](https://github.com/awslabs/aws-glue-schema-registry/issues/101).
- **A single-type `allOf` is unwrapped instead of read as a union.** everit loads a schema by
  keyword group, so `format` — a string keyword — pulls a `StringSchema` out of any schema
  that carries it, whatever the declared type. `{"type": "integer", "format": "int64"}` loads
  as `CombinedSchema allOf [StringSchema {}, NumberSchema integer]`, and the extra subschema
  is an artefact of that split: it constrains nothing, since everit builds it with
  `requiresString` false. `JsonSchemaToConnectSchemaConverter` treated every `CombinedSchema`
  as a union, so the field became a `…Oneof` struct with two _required_ branches, and the
  first record failed with
  `DataException: Did not find matching union field for data: 1627335205`.
  `allOf` is a conjunction, not a union, so it is now reduced first: subschemas
  that carry no keyword at all are dropped, and a conjunction left with exactly one subschema
  is replaced by it. The reduction feeds the type converter only — `connect.type`,
  `connect.name`, `connect.doc`, `connect.version`, `connect.parameters` and `default` are
  read from the outer schema as before, which is where everit puts them. An `allOf` with two
  subschemas that do carry keywords is untouched, and so is every `oneOf` and `anyOf`. This is
  [awslabs/aws-glue-schema-registry#369](https://github.com/awslabs/aws-glue-schema-registry/issues/369).
