#!/usr/bin/env sh
# Prove the repository works from exactly what git tracks.
#
# Exports HEAD to a temp dir (so ignored/untracked files are absent), then
# runs the README's first two commands there: make install-dev && make check.
# Catches the class of bug where the working tree relies on a file that never
# reached git (a monorepo *.json ignore has swallowed fixtures and the
# devcontainer before). Run before tagging a release (CONTRIBUTING).
#
#   scripts/check_fresh_tree.sh            # uses HEAD
#   scripts/check_fresh_tree.sh <rev>
#
# Leaves the temp dir in place on failure and prints its path.

set -eu
rev="${1:-HEAD}"
root="$(git rev-parse --show-toplevel)"
work="$(mktemp -d "${TMPDIR:-/tmp}/cloudscale-fresh.XXXXXX")"
echo "exporting $rev -> $work"
git -C "$root" archive --format=tar "$rev" | tar -x -C "$work"
cd "$work"
# The harness self-test asserts a real revision; give the export a git identity.
git init -q && git add -A && git -c user.email=fresh@check -c user.name=fresh commit -qm "fresh-tree export of $rev"
echo "== make install-dev"
make install-dev > install.log 2>&1 || { echo "INSTALL FAILED; see $work/install.log"; exit 1; }
echo "== make check"
if make check > check.log 2>&1; then
  tail -1 check.log
  echo "fresh tree OK ($work)"
else
  echo "CHECK FAILED; see $work/check.log"
  grep -E "^E  |FAILED|error" check.log | head -10
  exit 1
fi
