# aws-glue-schema-registry

Mobsuccess fork of [`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry),
reduced to the Java part and ported to Gradle. The conversion of the code to Kotlin is
in progress.

## Port status

| Stage                        | Status      |
| ---------------------------- | ----------- |
| C# and native module removed | done        |
| Maven → Gradle migration     | done        |
| Java → Kotlin conversion     | in progress |

The first commit of the repository is the `awslabs` source verbatim (`eed1506`). Every
deviation reads with `git diff eed1506 -- <path>`. Accepted deviations are listed in
[docs/portage.md](docs/portage.md).

## The golden rule

The repository must stay **behaviour-identical to the source at the Java level**. The
inherited test suite is the only guard rail: **2087 tests, zero failure**. A conversion
step that lowers that total or breaks a test is not finished, however good the produced
code looks.

```bash
./gradlew clean build     # compile + 2087 tests + jars
./gradlew test            # tests only
./gradlew assemble        # jars only
```

## Conversion method

Kotlin and Java compile together within a module. `main` sources are therefore converted
**while the tests stay in Java**: the inherited suite has not moved and acts as an oracle
for the converted code. Tests are converted in a second pass, once all of `main` is done.

Work module by module, in dependency order, and run the **whole** build before committing:
a module's own tests do not cover the modules that consume it.

## Structure

Ten modules, whose directories mirror those of the source repository and whose Gradle
project names reuse the Maven artifactIds:

| Directory                           | Artifact                                 | Role                                     |
| ----------------------------------- | ---------------------------------------- | ---------------------------------------- |
| `common`                            | `schema-registry-common`                 | Glue client, cache, exceptions           |
| `serializer-deserializer`           | `schema-registry-serde`                  | SerDe core (Avro, JSON Schema, Protobuf) |
| `serializer-deserializer-msk-iam`   | `schema-registry-serde-msk-iam`          | uber-jar SerDe + MSK IAM auth            |
| `kafkastreams-serde`                | `schema-registry-kafkastreams-serde`     | Kafka Streams integration                |
| `avro-kafkaconnect-converter`       | `schema-registry-kafkaconnect-converter` | Connect Avro converter                   |
| `avro-flink-serde`                  | `schema-registry-flink-serde`            | Flink (de)serialization schemas          |
| `jsonschema-kafkaconnect-converter` | `jsonschema-kafkaconnect-converter`      | Connect JSON Schema converter            |
| `protobuf-kafkaconnect-converter`   | `protobuf-kafkaconnect-converter`        | Connect Protobuf converter               |
| `examples`                          | `schema-registry-examples`               | integration examples                     |
| `integration-tests`                 | `schema-registry-integration-tests`      | tests requiring real AWS resources       |

The graph is linear: `common` → `serializer-deserializer` → all the others. That is the
order to follow for the Kotlin conversion.

## Build

- Gradle 9.6.1, Kotlin DSL, **JVM 17** toolchain (consumable by Kafka Connect and Flink).
  CI installs 17 and 21: 21 runs Gradle itself, 17 is the compilation toolchain.
- Versions centralized in `gradle/libs.versions.toml` — never hard-code a version in a
  `build.gradle.kts`
- Shared configuration in `buildSrc/src/main/kotlin/gsr.*.gradle.kts`, no `subprojects {}`
  in the root build
- Published to GitHub Packages under the `com.mobsuccess` group

## Java interop traps

These all cost a red test at least once. They are listed in the order they bite.

- **Kotlin classes and methods are final by default**, unlike their Java counterparts. Any
  type a test mocks needs `open` on the class _and_ on every stubbed method.
- **`@NonNull` raised an `IllegalArgumentException`**, not a `NullPointerException`, because
  of `lombok.nonNull.exceptionType` in `lombok.config`. Converting to a non-nullable type
  changes the exception type; update the asserting test rather than weakening the signature.
- **Kotlin does not see Lombok-generated accessors** on the Java classes left to convert; it
  resolves the property name to the private field instead. The Kotlin Lombok plugin, applied
  in the conventions, fixes this and stays necessary until the migration ends.
- **Lombok's `@Builder` has no Kotlin equivalent.** Rewrite it as a nested `Builder` class
  plus a `@JvmStatic builder()`, so the API seen from Java stays identical.
- **`@Data` also generated `equals`/`hashCode`/`toString`.** Omitting them silently falls
  back to identity comparison.
- **Boolean accessors:** Lombok generates `isXxx()` for a `boolean xxx` field; Kotlin
  generates `getXxx()` unless the property itself is named `isXxx`.
- **Enums cannot redeclare `name`.** Rename the backing property and expose `getName()`.
- **Checked exceptions vanish** without `@Throws`, and Java callers catching them stop
  compiling.
- **A Kotlin `inner` class cannot hold a companion object.** Move its constants to the outer
  companion.
- **Private functions get no parameter null checks**, unlike public ones.
- **Kotlin does not widen `int` to `long` implicitly**, nor infer generic variance the way
  javac did.
- **A cast can be optimized away.** `(value as CharSequence).toString()` resolves `toString()`
  on `Any`, so the checkcast is elided and a wrong-typed value is silently accepted where the
  Java code threw `ClassCastException`. Bind the cast to a typed local when the cast itself is
  the check.
- **Collection ordering is observable.** `HashSet`/`HashMap` iteration order ends up in
  rendered JSON and in schema equality; replacing them with Kotlin's order-preserving `setOf`
  or `mapOf` changes the output.
- **The order of `instanceof` branches matters** when the types are related — `EnumSchema`
  extends `StringSchema`, so reordering a `when` changes which converter is selected.
- **`String.split` does not drop trailing empty parts** the way Java's does. `"a/b/".split("/")`
  yields three elements in Kotlin and two in Java; append `.dropLastWhile { it.isEmpty() }`
  wherever the Java code relied on that trimming.
- **`internal` members of a Kotlin dependency are unreachable.** Wire's `ProtoParser`
  constructor is `internal`: Java saw it as public, Kotlin does not. Look for the public
  entry point that wraps it rather than working around the visibility.
- **Java's package-private and `protected` nested types have no Kotlin equivalent** when a
  public method exposes them. Make the class public with an `internal` constructor.
- **A test cannot hand a literal `null` to a non-nullable parameter.** The tests that assert
  a null is rejected have to route it through an erased generic — `nullOf()` in
  `TestNulls.kt` — so the check that fires is still the callee's own, not one the test
  performed on its behalf.
- **`@MethodSource` resolves by JVM name.** A provider must be `@JvmStatic` in a companion
  object and must not be `internal`: name mangling makes JUnit report the method as missing.
- **`Mockito.any(Foo.class)` returns null**, which a Kotlin non-nullable parameter rejects
  before the mock is ever reached. Use mockito-kotlin's `any<Foo>()`, which hands back a
  non-null stand-in; the same applies to `anyMap()` and `anyString()`.

## Other build notes

- **Lombok is still active** for as long as code remains in Java. Every class converted to
  Kotlin must drop its Lombok annotations in favour of native equivalents.
- **`org.lz4:lz4-java` is excluded globally** in favour of `at.yawk.lz4:lz4-java`. Both
  declare the same _capability_; reintroducing the former breaks resolution.
- **Code generation**: protobuf (`serializer-deserializer`, `protobuf-kafkaconnect-converter`)
  and Avro (`avro-kafkaconnect-converter`). Generated sources are not versioned.
- **`serializer-deserializer` publishes a `tests` jar** consumed by `integration-tests`
  through the `testArtifacts` configuration.
- The Kotlin dependencies pulled in by `mbknor-jackson-jsonschema` and `wire` are pinned at
  `1.9.25` (`kotlinRuntime` in the catalog): that is distinct from the compiler version.

## Conventions

- Everything that lands on GitHub is written in **English**: commit messages, pull request
  titles and bodies, code comments, documentation.
- Kotlin lint: ktlint 1.4.1, configured in `.editorconfig`, `intellij_idea` style
- Local hooks: `pre-commit install` (prettier, ktlint, end-of-file)
- **The pull request title drives the version number.** Merges are squashed with `PR_TITLE`
  as the commit message, and `scripts/version.sh` derives the bump from conventional commits
  since the last `v<major>.<minor>.<patch>` tag. A title with no recognized prefix silently
  yields a patch bump.

  | Title prefix                                              | Bump  |
  | --------------------------------------------------------- | ----- |
  | `feat!:`, or a `BREAKING CHANGE:` footer line in the body | major |
  | `feat:`                                                   | minor |
  | `fix:`, `chore:`, `docs:`, anything else                  | patch |

  The Kotlin conversion therefore ships under `feat!:` to come out as 2.0.0.

- Versions: the git tag carries the `v` prefix (`v1.0.0`), the Maven version does not
  (`1.0.0`). The prefix is not cosmetic — `version.sh` runs `git describe --match 'v*'`
  then strips the `v`; without it, every release would restart from 1.0.0.
- `.mobsuccess.yml` disables the `linear`, `ms-testers`, `mobsuccess`, `closed` and `python`
  workflows: this repository does not require a Linear ticket per pull request.

## Supply chain

The build resolves third-party code, and CI runs it with a GitHub token. These rules keep
that surface small; none of them survive a careless rewrite, so check them before touching
`.github/`, `settings.gradle.kts` or the wrapper.

- **GitHub Actions are pinned to a commit SHA**, never a tag: a tag is mutable and moving
  it is how `tj-actions/changed-files` was compromised. The trailing `# vX.Y.Z` comment is
  the human-readable part and Dependabot rewrites both together — do not replace the SHA
  with the tag.
- **The Gradle wrapper is checksummed twice**: `distributionSha256Sum` in
  `gradle-wrapper.properties` pins the distribution zip, and `gradle/actions/wrapper-validation`
  checks the committed `gradle-wrapper.jar` against Gradle's published hashes. Both have to
  be refreshed together when the wrapper is upgraded, with the values from
  `services.gradle.org/distributions/gradle-<version>-bin.zip.sha256`.
- **No job that runs repository code holds a write token.** `Gradle Build` runs third-party
  plugin code and is limited to `contents: read` with `persist-credentials: false`, so a
  hostile dependency finds neither a token in `.git/config` nor one it could comment or push
  with. Keep the pull request comments in `report`, which downloads artifacts and runs no
  repository code — `report` needs `contents: read` on top of its write scopes, since
  `publish-unit-test-result-action` resolves the commit through the API. `permissions: {}` at
  the top of the workflow means a new job starts with nothing, since the repository-wide
  default is still `write`. The exceptions are deliberate:
  `ktlint` takes `pull-requests: read` because reviewdog reads the diff to place its
  findings, and the two `publish-*` jobs need `packages: write` to publish — plus, for
  `publish-release`, `contents: write` and the only checkout that keeps its credentials
  persisted, since its last step pushes the release tag. Those two run on a push to `master`
  or `prod`, never on a pull request.
- **`report` tolerates a build that produced nothing.** Its downloads are
  `continue-on-error`, and each publishing step is gated on its own download having
  succeeded: a compile failure leaves `Gradle Build` red on its own rather than dragging a
  second job down with it. The job itself runs on `!cancelled()`, so a failing test suite is
  still reported.
- **Dependabot proposes a release only once it has aged** (`cooldown` in
  `dependabot.yml`): a hijacked publish is usually pulled within days. The cooldown covers
  version updates only — a security update still lands the day it is published, which is
  the intent. The schedule is monthly and the groups are wide, so the batch arrives as a
  handful of pull requests; a dependency lands in the **first** group it matches, which is
  why the `minor-and-patch` catch-all comes last and why majors, excluded from it, keep an
  individual pull request.
- **`RepositoriesMode.FAIL_ON_PROJECT_REPOS`** in `settings.gradle.kts` turns a module
  declaring its own repository into an error rather than a silent addition to the
  resolution order.
- **Dependency locking is deliberately absent.** The catalog fixes every direct version with
  no range and Maven Central is immutable, so resolution is already deterministic and a
  transitive only moves inside a reviewable commit. Lockfiles would add a manual
  `--write-locks` to every bump, since Dependabot cannot regenerate them.
  `gradle/verification-metadata.xml` is the control that would add something, and it is not
  wired yet.

## Integration tests

The `integration-tests` module requires real AWS resources. Its `*IntegrationTest` classes
are excluded from the unit run by the build convention — do not re-enable them in CI.
