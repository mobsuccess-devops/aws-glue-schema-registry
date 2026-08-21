#!/usr/bin/env bash
#
# Calculate the next semantic version based on conventional commits since the last tag.
#
# Usage: ./scripts/version.sh
#
# Output (on stdout):
#   BUMP=minor
#   VERSION=1.2.0
#
# Conventional commits rules:
#   fix:                          → patch
#   feat:                         → minor
#   feat!: / fix!:                → major
#   BREAKING CHANGE in commit body → major
#   chore:/docs:/ci:/etc.         → patch

set -euo pipefail

LATEST_TAG=$(git describe --tags --abbrev=0 --match 'v[0-9]*.[0-9]*.[0-9]*' 2>/dev/null || echo "")

# If no tag exists, first release is 1.0.0
if [ -z "$LATEST_TAG" ]; then
  echo "BUMP=first"
  echo "VERSION=1.0.0"
  exit 0
fi

CURRENT="${LATEST_TAG#v}"

if [[ ! "$CURRENT" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "version.sh: latest tag '${LATEST_TAG}' is not a v<major>.<minor>.<patch> release tag." >&2
  exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

RANGE="${LATEST_TAG}..HEAD"

SUBJECTS=$(git log "$RANGE" --pretty=format:"%s" 2>/dev/null || echo "")
BODIES=$(git log "$RANGE" --format=%B 2>/dev/null || echo "")

if [ -z "$SUBJECTS" ]; then
  echo "BUMP=patch"
  echo "VERSION=${MAJOR}.${MINOR}.$((PATCH+1))"
  exit 0
fi

if grep -qE '^[a-z]+(\(.+\))?!:' <<< "$SUBJECTS" \
  || grep -qE '^BREAKING[ -]CHANGE:' <<< "$BODIES"; then
  BUMP="major"
elif grep -qE '^feat(\(.+\))?:' <<< "$SUBJECTS"; then
  BUMP="minor"
else
  BUMP="patch"
fi

case $BUMP in
  major) VERSION="$((MAJOR+1)).0.0" ;;
  minor) VERSION="${MAJOR}.$((MINOR+1)).0" ;;
  patch) VERSION="${MAJOR}.${MINOR}.$((PATCH+1))" ;;
esac

echo "BUMP=$BUMP"
echo "VERSION=$VERSION"
