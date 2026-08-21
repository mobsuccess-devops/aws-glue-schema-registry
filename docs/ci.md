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

## Branch protection

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
  turned off without editing it. `actions/dependency-review-action` needs the same feature
  and could now be added.
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
