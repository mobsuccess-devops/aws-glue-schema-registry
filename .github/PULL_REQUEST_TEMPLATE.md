<!--
The title is the release note and the version bump: the merge is squashed with it as the
commit message, and an unrecognised prefix silently yields a patch bump.

| Title prefix                                              | Bump  |
| --------------------------------------------------------- | ----- |
| `feat!:`, or a `BREAKING CHANGE:` footer line in the body | major |
| `feat:`                                                   | minor |
| `fix:`, `chore:`, `docs:`, anything else                  | patch |
-->

## Why

<!-- The reasoning, not the diff. Reviewers can read the diff. -->

## Checklist

- [ ] `./gradlew clean build` is green and the test count has not gone down
- [ ] `.api` diffs are deliberate and justified above — a **removed** symbol means asking whether the title should be `feat!:`
- [ ] A change in observable behaviour has its entry in `docs/portage.md`
- [ ] English everywhere: title, body, documentation
