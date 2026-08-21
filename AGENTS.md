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
inherited test suite is the only guard rail: **it must stay green, and it must never
shrink**. A conversion step that drops a test or turns one red is not finished, however
good the produced code looks.

```bash
./gradlew clean build     # compile + full test suite + jars
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

### The ABI is versioned

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

## Java interop traps

These all cost a red test at least once. They are listed in the order they bite.

- **Kotlin classes and methods are final by default**, unlike their Java counterparts. Any
  type a test mocks needs `open` on the class _and_ on every stubbed method.
- **`@NonNull` raised an `IllegalArgumentException`**, not a `NullPointerException`, because
  of `lombok.nonNull.exceptionType` in `lombok.config`. Converting to a non-nullable type
  changes the exception type; update the asserting test rather than weakening the signature.
- **Kotlin does not see Lombok-generated accessors** on a Java class it compiles alongside; it
  resolves the property name to the private field instead. The Kotlin Lombok plugin used to
  fix that in the conventions. It is gone: no Kotlin source consumes Lombok any more. Bring it
  back only if Lombok reappears in a module that also holds Kotlin.
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

- **Lombok is confined to `integration-tests`**, the one module still entirely in Java. It is
  declared there, on the test configurations only. The root `lombok.config` is what keeps
  `lombok.nonNull.exceptionType = IllegalArgumentException` in force for it, so it stays as
  long as that module does; the per-module copies are gone.
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
  (`1.0.0`). The prefix is not cosmetic — `version.sh` runs
  `git describe --match 'v[0-9]*.[0-9]*.[0-9]*'` then strips the `v`; without it, every
  release would restart from 1.0.0. That glob, and the semver check that follows it, are
  what keep a tag like `v.1.1.15` or `v1.2.08` out of the arithmetic.
- **`.mobsuccess.yml` is what mobsuccessbot enforces**, and it holds no comments — the file
  is rewritten by the bot, so the reasoning lives here instead.
  - It disables the `linear`, `ms-testers`, `mobsuccess`, `closed` and `python` workflows:
    this repository does not require a Linear ticket per pull request.
  - `requiredApprovingReviewCount: 0` and `isAdminEnforced: false` on `master` are
    deliberate — an agent-driven repository, with team Core able to force a merge. Same
    settings as panoramai.
  - `additional-required-job-names` exists because automatic detection only knows the jobs
    of the policy's template workflows; those of `ci.yml`, written by hand, are invisible to
    it, and without the list a red CI still leaves a pull request mergeable. Set these here,
    never in the branch-protection UI — the next policy run overwrites the UI.
  - The list names **resolved** job names, so the matrix appears as `Tests on JDK 21` and
    `Tests on JDK 25`. Both are required because `Gradle Build` runs on 17 only: they carry
    the guarantee that JSON Schema output does not depend on the JDK that produced it, and
    without them a regression visible only on 21 or 25 merges green.
  - `Analyze java-kotlin` is required **only because `ENABLE_CODEQL` is set** as a
    repository variable. The job is gated on that variable, and a required check that skips
    counts as green — so listing it while the variable is unset would look like a security
    gate without being one. Unset the variable and the entry has to go with it.

## Supply chain

The build resolves third-party code, and CI runs it with a GitHub token. These rules keep
that surface small; none of them survive a careless rewrite, so check them before touching
`.github/`, `settings.gradle.kts` or the wrapper.

- **GitHub Actions are pinned to a commit SHA**, never a tag: a tag is mutable and moving
  it is how `tj-actions/changed-files` was compromised. The trailing `# vX.Y.Z` comment is
  the human-readable part and Dependabot rewrites both together — do not replace the SHA
  with the tag.
- **The Gradle wrapper is checksummed twice**: `distributionSha256Sum` in
  `gradle-wrapper.properties` pins the distribution zip, and `validate-wrappers: true`
  checks the committed `gradle-wrapper.jar` against Gradle's published hashes before any
  Gradle code runs. Both have to be refreshed together when the wrapper is upgraded, with
  the values from `services.gradle.org/distributions/gradle-<version>-bin.zip.sha256`. Every
  job that runs Gradle sets that flag — on `gradle/actions/setup-gradle` in `ci.yml` and
  `codeql.yml`, on `gradle/actions/dependency-submission` in `dependency-submission.yml`,
  which takes the same option. A new job that runs `./gradlew` has to set it too.
- **The Gradle cache is written from a push to `master` only.** `setup-gradle` restores it
  everywhere but takes `cache-read-only` on anything else, so neither a pull request —
  including one from a fork — nor a push to `prod` can poison the cache a later `master`
  build restores. `test-jdk` and the two `publish-*` jobs are read-only unconditionally:
  they consume what `Gradle Build` produced.
- **`test-jdk` lists its JDKs newest-first for a reason.** `setup-java` makes the _last_
  entry `JAVA_HOME`, so the order `25, 17, 21` is what puts Gradle itself on 21 while
  keeping 17 as the compilation toolchain and leaving 25 available as a target to run the
  tests on. Reordering that list silently changes which JVM Gradle runs on.
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
- **The dependency graph is generated and submitted by two different jobs.** Dependabot
  alerts otherwise see only the direct versions of the catalog, never the transitives that
  actually carry most CVEs. Submitting the graph needs `contents: write`, and resolving it
  needs to run the build — so `dependency-submission.yml` splits the two, exactly as
  `ci.yml` splits `build` and `report`: `generate` resolves the graph with `contents: read`
  and uploads it as an artifact, `submit` posts that artifact and runs no repository code.
- **CodeQL is wired but dormant.** `codeql.yml` analyses `java-kotlin` with a manual build
  mode, and is gated on the `ENABLE_CODEQL` repository variable being `true`. Code scanning
  needs GitHub Code Security, which this private repository does not have: the gate keeps
  the workflow ready and out of the way instead of red on every run. Flip the variable once
  the feature is enabled. `actions/dependency-review-action` is deliberately absent for the
  same reason — it needs the same feature.
- **Releases are attested and checksummed.** `publish-release` builds a `SHA256SUMS.txt`
  over every jar it published and attaches it to the GitHub release, then
  `actions/attest-build-provenance` signs a provenance statement for those same jars. A
  consumer can check a jar pulled from GitHub Packages against the release checksums, or
  verify its build provenance with `gh attestation verify <jar> --repo <this repo>`. The
  job therefore also holds `id-token: write` and `attestations: write`.
- **Redundant runs are cancelled on pull requests only.** `build`, `test-jdk` and `ktlint`
  carry a `concurrency` group keyed on the pull request number — `test-jdk` keys on its
  matrix leg too, or the two legs would cancel each other — with `cancel-in-progress` scoped
  to `pull_request`. A push to `master` or `prod` is never cancelled: `publish-snapshot` and
  `publish-release` hang off `build`, and cancelling it would cancel the publication with
  it. `merge_group` is excluded for the same reason.
- **`RepositoriesMode.FAIL_ON_PROJECT_REPOS`** in `settings.gradle.kts` turns a module
  declaring its own repository into an error rather than a silent addition to the
  resolution order.
- **Dependabot is told to leave Flink alone.** The catalog pins `flink` at 1.12.2 because
  `flink-streaming-java_2.11` exists under no later coordinate: the Scala suffix was dropped
  upstream. Since both Flink artifacts share the same version reference, any bump Dependabot
  proposed would either fail to resolve or dead-end at 1.14.6. The `ignore` entry on
  `org.apache.flink:*` keeps that pull request from being opened; moving off 1.12.2 is a
  deliberate migration, not a version bump.
- **Dependency locking is deliberately absent.** The catalog fixes every direct version with
  no range and Maven Central is immutable, so resolution is already deterministic and a
  transitive only moves inside a reviewable commit. Lockfiles would add a manual
  `--write-locks` to every bump, since Dependabot cannot regenerate them.
  `gradle/verification-metadata.xml` is the control that would add something, and it is not
  wired yet.

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
