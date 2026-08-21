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
[docs/portage.md](docs/portage.md) saying what changed and why, and a line in
[CHANGELOG.md](CHANGELOG.md) if a consumer could notice it.

The inherited test suite is the oracle: **2097 tests, zero failures**. A change that lowers
that total or breaks a test is not finished, however good the code looks. If a test has to
change, the pull request has to explain why the old assertion was wrong.

## Building and testing

JVM 17 or later; Gradle resolves its own toolchain.

```bash
./gradlew clean build     # compile, run the 2097 tests, produce the jars
./gradlew test            # tests only
./gradlew assemble        # jars only
```

The `integration-tests` module needs real AWS resources. Its `*IntegrationTest` classes are
excluded from the unit run and from CI — do not re-enable them there.

Install the hooks once, so formatting is fixed before it reaches review:

```bash
pre-commit install
```

They run prettier, ktlint 1.4.1 (configured in `.editorconfig`, `intellij_idea` style) and an
end-of-file fixer.

## House style

- **English everywhere that lands on GitHub**: commit messages, pull request titles and
  bodies, documentation and any comment.
- **No comments in the code.** Rationale goes in the commit message, in the pull request
  body, or in `docs/portage.md` — somewhere it can be maintained. Code that needs a comment
  to be understood usually needs a better name instead.
- **Versions live in `gradle/libs.versions.toml`.** Never hard-code one in a
  `build.gradle.kts`.
- **Kotlin, for new code.** The Java → Kotlin conversion is done except for
  `integration-tests`. [AGENTS.md](AGENTS.md) lists the Java interop traps that have each
  cost a red test at least once; it is worth reading before converting anything.

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
