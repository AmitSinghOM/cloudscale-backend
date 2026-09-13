#!/usr/bin/env sh
# Fail when a lock file has not been regenerated for too long.
#
# LONGEVITY §3: dependencies age deliberately. Age is the time since the file
# last changed in git (so a no-op touch does not count; only a real
# regeneration does). Warn at WARN_DAYS, fail at MAX_DAYS.
#
#   scripts/check_lock_age.sh requirements.lock requirements-dev.lock
#
# Override thresholds with LOCK_WARN_DAYS / LOCK_MAX_DAYS (e.g. in a
# reproduction). Requires full git history (CI: actions/checkout fetch-depth 0).

set -eu

WARN_DAYS="${LOCK_WARN_DAYS:-90}"
MAX_DAYS="${LOCK_MAX_DAYS:-120}"
now="$(date +%s)"
status=0

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <lock file>..." >&2
  exit 2
fi

for lock in "$@"; do
  if [ ! -f "$lock" ]; then
    echo "FAIL $lock: file not found" >&2
    status=1
    continue
  fi
  last="$(git log -1 --format=%ct -- "$lock" 2>/dev/null || true)"
  if [ -z "$last" ]; then
    echo "FAIL $lock: no git history (shallow clone? untracked?)" >&2
    status=1
    continue
  fi
  age_days=$(( (now - last) / 86400 ))
  if [ "$age_days" -gt "$MAX_DAYS" ]; then
    echo "FAIL $lock: last regenerated ${age_days} days ago (limit ${MAX_DAYS})." >&2
    echo "     Regenerate with the command in the file header, run the gate, commit." >&2
    status=1
  elif [ "$age_days" -gt "$WARN_DAYS" ]; then
    echo "WARN $lock: ${age_days} days old (warn ${WARN_DAYS}, fail ${MAX_DAYS})."
  else
    echo "OK   $lock: ${age_days} days old."
  fi
done

exit "$status"
