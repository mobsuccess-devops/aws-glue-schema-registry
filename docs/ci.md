# CI, releases and supply chain

## Releases

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

## Publication

Two channels, two audiences.

| Channel                                                                                                  | Contents               | Auth to read | Published by                        |
| -------------------------------------------------------------------------------------------------------- | ---------------------- | ------------ | ----------------------------------- |
| [Maven Central](https://central.sonatype.com/namespace/com.mobsuccess)                                   | releases only          | none         | `publishAggregationToCentralPortal` |
| [GitHub Packages](https://github.com/orgs/mobsuccess-devops/packages?repo_name=aws-glue-schema-registry) | releases and snapshots | classic PAT  | `publish`                           |
| [GitHub Releases](https://github.com/mobsuccess-devops/aws-glue-schema-registry/releases)                | the four plugin zips   | none         | `pluginDistribution` + `gh release` |

`publish-snapshot` is unchanged: a push to `master` publishes `<next>-SNAPSHOT` to GitHub
Packages and nothing else. `publish-release` runs both, **Central first**, then GitHub
Packages. The order is deliberate: GitHub Packages refuses to overwrite a release version it
already holds, so a Central failure after a successful GitHub Packages publish would leave an
untagged version behind and wedge the retry on the version number it recomputes. Central
first means a failure there leaves nothing published anywhere, and a re-run starts clean.

- **The Central path is `com.gradleup.nmcp`, not `com.vanniktech.maven.publish`.** Both
  cover the Portal API; they differ in what they do to the publications. vanniktech _creates_
  the publication from a platform descriptor (`JavaLibrary`, `KotlinJvm`) and owns the artifact
  set, the sources and javadoc jars and the pom. nmcp creates nothing: it reads the
  `maven-publish` publications the convention plugins already produce, stages them into a local
  repository and uploads the zip. That was decisive while the four shaded modules owned their
  own artifact set and pom; now that every module publishes the same way it is simply the
  smaller dependency, and there is no reason to change it.
- **Only the nine library modules go to Central.** `nmcpAggregation(project(…))` in the root
  build names them one by one. `schema-registry-examples` publishes to GitHub Packages as it
  always has, but it is not a library and Central is immutable — an artifact put there cannot
  be taken back.
- **Signing is conditional on the key being present.** Central rejects unsigned artifacts, so
  `gsr.publish-conventions` configures `signing` with `useInMemoryPgpKeys` — but only when
  `signingInMemoryKey` resolves. A local `./gradlew build` and a pull request build have no
  key, register no `Sign` task and need nothing. The release job supplies the key and every
  artifact, the pom and the module metadata get an `.asc` next to them.
- **Javadoc comes from Dokka.** Central requires a javadoc jar; the sources are Kotlin, so
  `javadocJar` packages the output of `dokkaGeneratePublicationJavadoc`. It is not wired into
  `assemble`, so it costs nothing on a pull request build and is built on the publish path.
- **The credentials are project properties, passed as `ORG_GRADLE_PROJECT_*`.** Gradle maps
  that environment prefix onto project properties by itself, so the workflow needs no glue.

  | Repository secret                | Environment variable                            | What it is                        |
  | -------------------------------- | ----------------------------------------------- | --------------------------------- |
  | `MAVEN_CENTRAL_USERNAME`         | `ORG_GRADLE_PROJECT_mavenCentralUsername`       | Portal **user token** username    |
  | `MAVEN_CENTRAL_PASSWORD`         | `ORG_GRADLE_PROJECT_mavenCentralPassword`       | Portal **user token** password    |
  | `SIGNING_IN_MEMORY_KEY`          | `ORG_GRADLE_PROJECT_signingInMemoryKey`         | ASCII-armored PGP **private** key |
  | `SIGNING_IN_MEMORY_KEY_PASSWORD` | `ORG_GRADLE_PROJECT_signingInMemoryKeyPassword` | its passphrase                    |

  The account password is never one of them: the Portal issues a token pair for publishing.

- **A release publishes without a human step.** `publishingType` defaults to `AUTOMATIC`: the
  bundle is uploaded, validated and published, and the task waits for the deployment to reach
  `PUBLISHED` before the job moves on — so a green `publish-release` means the version landed
  on Central, and a red one means it did not. Passing `-PcentralPublishingType=USER_MANAGED`
  restores the manual gate, parking the deployment in
  [Deployments](https://central.sonatype.com/publishing/deployments) for a maintainer to press
  **Publish**.

  What that gives up is worth stating, because nothing else covers it: the portal screen was
  the last place to catch a **wrong version number** before it became permanent. `version.sh`
  derives the version from the pull request title, and a title with no recognized prefix
  silently yields a patch bump — a mistake no test can see and Central cannot undo, since a
  published version is never replaced or removed. Automatic publishing trades that one check
  for one less step.

- **The bundle can be built and checked without publishing anything.**

  ```bash
  PACKAGE_VERSION=<version> ./gradlew nmcpZipAggregation nmcpCheckAggregationFiles
  ```

  It stages every publication into `build/nmcp/zip/aggregation.zip` and verifies that each
  coordinate carries the files Central requires. No credentials, no upload.

- **A publication weighs 7.9 MB across 204 files**, which is what the plugin distributions
  being release assets rather than Maven artifacts buys. Maven Central meters the free tier on
  release size — a 78 MB monthly threshold, evaluated as a three-month average and rate-limited
  from 1 October 2026 — and the four uber-jars this repository used to publish were 276 MB of a
  284 MB release on their own. The reasoning, the measurements and the alternatives that were
  weighed are in [build.md](build.md#the-plugin-distributions).

- **The plugin zips go to the GitHub Release, not to a Maven repository.** `pluginDistribution`
  is wired into `assemble` but not into any publication, so `publish` and
  `publishAggregationToCentralPortal` never see it. The release job builds the four zips,
  covers them with the same `SHA256SUMS.txt` and build-provenance attestation as the jars, and
  attaches them to the tag. Release assets are anonymous to download and are not metered, which
  is the whole point.

- **They are built before either publish step, for the reason the Central-first order exists.**
  Both publications are irreversible and the tag is written last, so a step that fails between
  them leaves a version published with no tag — and the retry recomputes the same number, which
  GitHub Packages then refuses to overwrite. `pluginDistribution` needs no credentials and can
  fail for ordinary build reasons, so it runs while a failure still costs nothing.

## Branch protection

- **`.mobsuccess.yml` is what mobsuccessbot enforces**, and it holds no comments — the file
  is rewritten by the bot, so the reasoning lives here instead.

  - It disables the `linear`, `ms-testers`, `mobsuccess`, `closed` and `python` workflows:
    this repository does not require a Linear ticket per pull request.
  - `requiredApprovingReviewCount: 0` on `master` is deliberate — an agent-driven
    repository, worked solo most days, and nobody can approve their own pull request:
    requiring one review would park every merge on a third party. The org-level
    `Force copilot review` ruleset still puts a Copilot review on each pull request, which
    reads the diff without gating the merge.
  - `isAdminEnforced: true` is what keeps that from being a hole. With no review required,
    an administrator could otherwise push straight to `master`, or merge over a red check:
    the required checks were the only gate, and admins were exempt from it. They are not
    any more. Set it in **both** places — the bot owns the value, so the file is what makes
    it durable, and `POST .../branches/master/protection/enforce_admins` is what makes it
    effective before the next policy run.
  - `additional-required-job-names` exists because automatic detection only knows the jobs
    of the policy's template workflows; those of `ci.yml`, written by hand, are invisible to
    it, and without the list a red CI still leaves a pull request mergeable. Set these here,
    never in the branch-protection UI — the next policy run overwrites the UI.
  - `Tests on the protobuf floor` guards the version contract described in
    [build.md](build.md): it is the only job that resolves protobuf at the floor the
    generated code imposes on consumers, so without it a floor raised past a consumer's own
    pin merges green.
  - The list names **resolved** job names, so the matrix appears as `Tests on JDK 21` and
    `Tests on JDK 25`. Both are required because `Gradle Build` runs on 17 only: they carry
    the guarantee that JSON Schema output does not depend on the JDK that produced it, and
    without them a regression visible only on 21 or 25 merges green.
  - `Analyze java-kotlin` is required **only because `ENABLE_CODEQL` is set** as a
    repository variable. The job is gated on that variable, and a required check that skips
    counts as green — so listing it while the variable is unset would look like a security
    gate without being one. Unset the variable and the entry has to go with it.
  - `ktlint` was advisory from the start — reviewdog reported at `level: warning` with
    `fail_on_error` unset, so the job was green whatever ktlint found, and it was not in the
    list either. It now runs at `level: error` with `fail_on_error: true`, and is required.

- **`prod` requires the same five checks as `master`**, and that is what makes the release
  gate real: without them, an administrator pushing a commit that never saw a pull request
  got a green `publish-release` and a tagged release out of it. The branch is not in
  `.mobsuccess.yml` — the bot only manages `master` — so `prod` is set through
  `PUT .../branches/prod/protection` and stays hand-held.
- **The release push still works because a check run belongs to a commit, not to a
  branch.** `master → prod` is a fast-forward of a SHA that already carries the five green
  checks from its own push to `master`, and branch protection evaluates the SHA being
  pushed, so the gate is already satisfied when the push arrives. It matters that this is
  the mechanism: `codeql.yml` does not even trigger on a push to `prod`, and the release
  would deadlock waiting for a run that never starts if the check had to be produced on the
  branch. The one consequence to know is timing — pushing to `prod` before `master`'s own
  post-merge run finishes leaves the checks pending and the push refused until they go
  green.
- **`prod` takes `strict: false` where `master` takes `strict: true`.** "Up to date before
  merging" answers a question `prod` does not have: nothing is merged into it, it is
  fast-forwarded. Enabling it there would only add a way for the release push to be
  refused.

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
  job that runs Gradle sets that flag — on `gradle/actions/setup-gradle` in `ci.yml`,
  `codeql.yml` and `integration.yml`, on `gradle/actions/dependency-submission` in
  `dependency-submission.yml`, which takes the same option. A new job that runs `./gradlew`
  has to set it too. `integration.yml` had neither: it ran Gradle off `setup-java`'s own
  `cache: gradle`, which restores a cache but validates nothing, and it referenced its four
  actions by tag. It was also the only workflow handing `checks: write` to a third-party
  action pinned to a moving tag.
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
  findings — it no longer takes `checks: write`, see the `github-pr-annotations` note
  below — and the two `publish-*` jobs need `packages: write` to publish — plus, for
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
- **The submitted graph covers the build's own tools, so their CVEs land as alerts too.**
  `dependency-submission` resolves every configuration of every project plus `buildSrc`, and
  a Gradle plugin drags its own transitives in: the alert then names `settings.gradle.kts`
  with no hint that nothing it points at is published. Three chains produced fifteen open
  alerts at once and are pinned down where each is resolved. `buildSrc` carries the
  `jackson-bom` platform and constraints on `avro`, `avro-compiler`, `commons-lang3` and
  `commons-compress`, which is what lifts `gradle-avro-plugin` 1.9.1 off Avro 1.11.3 —
  hence the plugin being a `buildSrc` dependency applied by id rather than a catalog
  `alias`, since only a dependency of a real project takes constraints.
  `gsr.publish-conventions` constrains `dokkaJavadocGeneratorRuntime`, Dokka 2.2.0's
  generator classpath, which is the last jsoup and the last Jackson 2.15 in the tree. And
  `mbknor-jackson-jsonschema` pulls ClassGraph 4.8.21, excluded in every module that
  declares it — `serializer-deserializer` did so from the start, the two test-only
  consumers did not. Check a new alert against `./gradlew dependencyInsight` before
  treating it as a runtime one; the resolved runtime classpaths carry none of these.
- **CodeQL builds with `--no-build-cache`, and that flag is load-bearing.** `codeql.yml`
  analyses `java-kotlin` in manual build mode, which extracts code from the compiler
  invocations `./gradlew assemble` makes. On a branch that touches no source, the restored
  Gradle build cache serves every `compileKotlin`/`compileJava` `FROM-CACHE`, no compiler
  ever runs, and the extractor fails the job on exit 32, having seen no source at all —
  which, since `Analyze java-kotlin` is required, blocked every documentation-only pull
  request. Disabling the build cache for that one invocation is what makes the analysis
  real; the dependency cache is untouched, so the cost is a full compile (~10 min) rather
  than a full download. Skipping the workflow on docs-only branches would
  have been the wrong fix: a required check that skips counts as green, which is the
  anti-pattern `.mobsuccess.yml` documents.
- **CodeQL is still gated on the `ENABLE_CODEQL` repository variable.** It is set now that
  the repository is public and has code scanning; the gate stays so the workflow can be
  turned off without editing it.
- **`dependency-review.yml` blocks a vulnerable dependency at the pull request**, where
  `dependency-submission.yml` only tells Dependabot what to alert on after the fact. It
  runs on `pull_request` alone, holds `contents: read` and nothing else — the action reads
  the dependency graph through the API and never needs to write — and takes
  `fail-on-severity: high`, so a high or critical advisory on a newly introduced dependency
  fails the job while the long tail stays with the Dependabot alerts that already cover the
  full transitive graph. It carries **no `ENABLE_*` gate**, unlike CodeQL: a gate whose
  variable is unset makes the job skip, a skipped required check counts as green, and the
  result would be a security gate that is not one. The job is not in
  `additional-required-job-names` yet — promoting it is a one-line change to
  `.mobsuccess.yml` once it has a few pull requests of history.
- **Releases are attested and checksummed.** `publish-release` builds a `SHA256SUMS.txt`
  over every jar it published and attaches it to the GitHub release, then
  `actions/attest-build-provenance` signs a provenance statement for those same jars. A
  consumer can check a jar pulled from Maven Central or GitHub Packages against the release
  checksums, or
  verify its build provenance with `gh attestation verify <jar> --repo <this repo>`. The
  job therefore also holds `id-token: write` and `attestations: write`.
- **Release tags are immutable, and the `Immutable release tags` ruleset is what the
  attestation above rests on.** A provenance statement binds a jar to the commit a tag
  pointed at; if the tag can be moved afterwards, the version a consumer resolves and the
  source that was attested drift apart without a trace. The ruleset targets `refs/tags/v*`
  with `deletion` and `non_fast_forward`, and deliberately **omits `creation`** —
  `publish-release` creates `v<version>` with the `GITHUB_TOKEN`, and rulesets apply to the
  `github-actions` bot like anyone else, so blocking creation would break every release.
  There are no bypass actors: re-cutting a botched tag means flipping the ruleset's
  `enforcement` to `disabled` for as long as it takes, which is the audit trail the bypass
  would not leave.
- **`ktlint` reports through `github-pr-annotations`, and that is what makes it a gate.**
  The `github-pr-check` reporter it used before opened a second check run of its own, also
  called `ktlint`, next to the job's — two checks under one name, which is no basis for a
  required context. `github-pr-annotations` writes the findings to the job's own log
  instead, so `ktlint` names exactly one check and the job's exit code is the verdict. It
  also drops the `checks: write` the old reporter needed. `filter_mode` is `nofilter`, not
  `file`: reviewdog resolves the changed files from the pull request diff, which does not
  exist on a `push` or a `merge_group` run, so `file` would have let a violation through the
  merge queue unseen. `nofilter` lints the whole tree on every event, which is only viable
  because the tree is clean — a violation left anywhere blocks every pull request until it
  is fixed.
- **Redundant runs are cancelled on pull requests only.** `build`, `test-jdk` and `ktlint`
  carry a `concurrency` group keyed on the pull request number — `test-jdk` keys on its
  matrix leg too, or the two legs would cancel each other — with `cancel-in-progress` scoped
  to `pull_request`. A push to `master` or `prod` is never cancelled: `publish-snapshot` and
  `publish-release` hang off `build`, and cancelling it would cancel the publication with
  it. `merge_group` is excluded for the same reason.
- **`RepositoriesMode.FAIL_ON_PROJECT_REPOS`** in `settings.gradle.kts` turns a module
  declaring its own repository into an error rather than a silent addition to the
  resolution order.
- **Dependabot may bump Flink, but not across a major.** The catalog used to pin `flink` at
  1.12.2 because `flink-streaming-java_2.11` existed under no later coordinate — the Scala
  suffix was dropped upstream — so every bump Dependabot could propose either failed to
  resolve or dead-ended at 1.14.6, and `org.apache.flink:*` was ignored outright. That
  reasoning expired with the move to 1.20.x LTS on the suffix-free coordinate: patches inside
  the LTS line are ordinary bumps and are let through. The `ignore` entry is now scoped to
  `version-update:semver-major`, because Flink 2.x drops APIs this module's consumers use and
  is a deliberate migration rather than a version bump.
- **Dependabot is held off Avro 1.12.2 and later.** The build is on Avro 1.12.1, which is a
  drop-in. 1.12.2 is not: it added `org.apache.avro.util.ClassSecurityValidator`, a
  **default-deny allowlist** for every class Avro resolves out of a schema. Only sixteen
  `java.lang` / `java.math` names are trusted and `org.apache.avro.SERIALIZABLE_PACKAGES` has no
  default, so every `SpecificRecord` path throws `SecurityException` until the consuming
  application sets that property — 130 failing tests here, and the same hardening is being
  reported against Camel and Pulsar. Moving to it is a migration with a consumer-facing
  contract change rather than a version bump, so the `ignore` entry is scoped to `>= 1.12.2`.
  Patches below that line flow normally.
- **Dependency locking is deliberately absent.** The catalog fixes every direct version with
  no range and Maven Central is immutable, so resolution is already deterministic and a
  transitive only moves inside a reviewable commit. Lockfiles would add a manual
  `--write-locks` to every bump, since Dependabot cannot regenerate them.
  `gradle/verification-metadata.xml` is the control that would add something, and it is not
  wired yet.
