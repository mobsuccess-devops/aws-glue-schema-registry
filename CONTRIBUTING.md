# Contributing

Thank you for considering a contribution. This repository is the Mobsuccess fork of
[`awslabs/aws-glue-schema-registry`](https://github.com/awslabs/aws-glue-schema-registry):
issues and pull requests belong **here**, not upstream, and there is no contributor licence
agreement to sign. By opening a pull request you agree that your contribution ships under
the [Apache License 2.0](LICENSE.txt), like the rest of the project.

## Before you start

- **Security problems do not go in an issue.** Follow [SECURITY.md](SECURITY.md).
- **Search first** — check the [open](https://github.com/mobsuccess-devops/aws-glue-schema-registry/issues)
  and [recently closed](https://github.com/mobsuccess-devops/aws-glue-schema-registry/issues?q=is%3Aissue+is%3Aclosed)
  issues.
- **Open an issue before a large change.** The fork has a narrow contract, described below;
  a change that crosses it is better discussed than rewritten.

## Reporting a bug

Useful reports say which artifact and version, which JVM, and which data format (AVRO, JSON
Schema, Protobuf). A failing test is worth more than a description — the suite is the
project's only guard rail, and a reproducer usually belongs in it.

If the behaviour also exists in the AWS artifact, say so: it changes the fix from a
conversion regression to an inherited bug, and the two are handled differently.

## The contract: identical by default, better where documented

The library must behave exactly like its upstream source at the Java level. That is what
makes the artifact a drop-in replacement, and it is the fork's whole value proposition.

Deliberate improvements are allowed — the fork carries several — but they are **documented
deviations, not silent ones**. A change in observable behaviour needs an entry in
[docs/portage.md](docs/portage.md) saying what changed and why, and — if a consumer could
notice it — a pull request title that says so, since that title is what ends up in the
release notes.

The inherited test suite is the oracle: **it must stay green, and the count must never go
down**. A change that drops a test or turns one red is not finished, however good the code
looks. If a test has to change, the pull request has to explain why the old assertion was
wrong.

## Building and testing

JVM 17 or later; Gradle resolves its own toolchain.

```bash
./gradlew clean build     # compile, run the full test suite, produce the jars
./gradlew test            # tests only
./gradlew assemble        # jars only
```

The `*IntegrationTest` classes need a Kafka broker and a Glue endpoint. They are excluded
from `test` and from `check` — do not re-enable them there — and are reached through the
separate `integrationTest` task instead:

```bash
./gradlew :schema-registry-kafkaconnect-converter:integrationTest       # needs nothing
./gradlew :schema-registry-integration-tests:integrationTestWithoutGlue # a broker and LocalStack
./gradlew :schema-registry-integration-tests:integrationTest            # the above, plus a Glue endpoint
```

`GLUE_ENDPOINT` and `KAFKA_BOOTSTRAP` point them at your own endpoints; unset, they fall back
to the values the upstream sources hard-coded. [integration.yml](.github/workflows/integration.yml)
runs the whole set nightly and on demand — never on a pull request — against a `motoserver/moto`
service container, so no AWS account is involved.

Install the hooks once, so formatting is fixed before it reaches review:

```bash
pre-commit install
```

They run prettier, ktlint 1.4.1 (configured in `.editorconfig`, `intellij_idea` style) and an
end-of-file fixer.

## The public ABI is frozen by the committed dumps

`binary-compatibility-validator` dumps the public ABI of every published module into
`<module>/api/<artifactId>.api`. Those files are committed, and `apiCheck` runs as part of
`check`: a change to a public signature fails `./gradlew build` until the dump is refreshed.

```bash
./gradlew apiCheck    # runs as part of `check`
./gradlew apiDump     # accept the new surface, then commit the .api diff
```

This is what mechanizes the fork's promise that the API stays identical to its source, so a
red `apiCheck` is not a formality — and it is the check most likely to fail a first
contribution. Read the diff it prints, decide whether the signature change is deliberate,
and only then run `apiDump`. The refreshed `.api` files are part of the pull request, and
the body has to say why the surface moved. A dump regenerated to turn the build green,
with no reasoning attached, is exactly what the check exists to catch.

**A dump that loses a line is a breaking change until argued otherwise.** A removed symbol
is something a consumer could have compiled against and no longer can, so the question the
pull request has to answer is whether the title should be `feat!:`. It has gone unasked
once: [#128](https://github.com/mobsuccess-devops/aws-glue-schema-registry/pull/128) dropped
the `$Companion` classes of three utility singletons from the dumps and shipped under
`refactor:` — a patch bump. The removal turned out to be defensible, because that
`Companion` was introduced by the Kotlin conversion and the Java original never had it, and
[docs/portage.md](docs/portage.md) now records the argument. But it was made after the
release, not before it. Make it in the pull request instead, while the title can still
change the version.

## House style

- **English everywhere that lands on GitHub**: commit messages, pull request titles and
  bodies, documentation and any comment.
- **No comments in the code.** Rationale goes in the commit message, in the pull request
  body, or in `docs/portage.md` — somewhere it can be maintained. Code that needs a comment
  to be understood usually needs a better name instead.
- **Versions live in `gradle/libs.versions.toml`.** Never hard-code one in a
  `build.gradle.kts`.
- **Kotlin, for new code.** The Java → Kotlin conversion is done except for
  `integration-tests`. [docs/kotlin-interop.md](docs/kotlin-interop.md) lists where Kotlin
  and Java do not line up; it is worth reading before writing Kotlin that Java has to see.

## Pull requests

Work from an up-to-date `master`, keep the change focused, and do not reformat unrelated
files — a diff that mixes a fix with a reformat is hard to review and hard to revert.

**The pull request title is the release note and the version bump.** Merges are squashed
with the title as the commit message, and `scripts/version.sh` derives the next version from
the conventional commits since the last `v*` tag:

| Title prefix                                              | Bump  |
| --------------------------------------------------------- | ----- |
| `feat!:`, or a `BREAKING CHANGE:` footer line in the body | major |
| `feat:`                                                   | minor |
| `fix:`, `chore:`, `docs:`, anything else                  | patch |

A title with no recognised prefix silently yields a patch bump, so a breaking change with a
careless title ships as a patch. Get the prefix right.

The body should explain **why**, not restate the diff. Reviewers can read the diff; they
cannot read the reasoning that produced it.

Then watch CI: `Gradle Build` is a required check, and ktlint comments inline.

## Releases

Maintainers only. A push to the `prod` branch triggers `publish-release`, which computes the
version, publishes to GitHub Packages, tags `vX.Y.Z` and creates the GitHub release.

## Code of conduct

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
