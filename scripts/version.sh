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
#   fix:           → patch
#   feat:          → minor
#   feat!: / fix!: → major
#   BREAKING CHANGE in commit body → major
#   chore:/docs:/ci:/etc.         → patch

set -euo pipefail

# Get latest tag
LATEST_TAG=$(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo "")

# If no tag exists, first release is 1.0.0
if [ -z "$LATEST_TAG" ]; then
  echo "BUMP=first"
  echo "VERSION=1.0.0"
  exit 0
fi

CURRENT="${LATEST_TAG#v}"
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

# Get commit messages since last tag
COMMITS=$(git log "${LATEST_TAG}..HEAD" --pretty=format:"%s" 2>/dev/null || echo "")

if [ -z "$COMMITS" ]; then
  echo "BUMP=patch"
  echo "VERSION=${MAJOR}.${MINOR}.$((PATCH+1))"
  exit 0
fi

BUMP="patch"
while IFS= read -r msg; do
  [ -z "$msg" ] && continue

  # Check for breaking change (! after type or BREAKING CHANGE in message)
  if echo "$msg" | grep -qE '^[a-z]+(\(.+\))?!:'; then
    BUMP="major"
    break
  elif echo "$msg" | grep -q 'BREAKING CHANGE'; then
    BUMP="major"
    break
  elif echo "$msg" | grep -qE '^feat(\(.+\))?:'; then
    [ "$BUMP" != "major" ] && BUMP="minor"
  fi
done <<< "$COMMITS"

case $BUMP in
  major) VERSION="$((MAJOR+1)).0.0" ;;
  minor) VERSION="${MAJOR}.$((MINOR+1)).0" ;;
  patch) VERSION="${MAJOR}.${MINOR}.$((PATCH+1))" ;;
esac

echo "BUMP=$BUMP"
echo "VERSION=$VERSION"
