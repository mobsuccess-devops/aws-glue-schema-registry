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

- **`templating-maven-plugin` replaced by a `Copy` task.** `common` declares
  `generateVersionSource`, which expands `src/main/java-templates` into the build directory
  so that `MavenPackaging.VERSION` carries the project version. That constant is read by the
  User-Agent interceptor of the Glue calls, so the value has to be the built version rather
  than a literal.
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
  stateful converter are not a value contract anyone can rely on — a converter put in a `HashSet`
  before `configure()` moves bucket once configured — and swapping a serializer out from under a
  running converter is not an operation this library should keep offering. The narrowing is real
  and is recorded here rather than reverted. `Schema`, the one `@Value` type among them, is a
  Kotlin `data class` and keeps all three overrides.

  The audit that prompted this entry also reported the opposite defect — that the JSON Schema
  converter caches, "privés en Java", had become public `val`s by accident. They had not:
  `ConnectSchemaToJsonSchemaConverter` and `JsonSchemaToConnectSchemaConverter` both carry
  `@Data`, so `getFromConnectSchemaCache()` and `getToConnectSchemaCache()` were already public,
  with setters. Making them `internal` would have narrowed the surface away from the source, not
  towards it. A sweep of every converted class for a field that is public in Kotlin and had no
  public accessor in Java — Lombok annotations accounted for — finds **no** accidental widening
  anywhere in the port.
