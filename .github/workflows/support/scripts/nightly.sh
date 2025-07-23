#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ---------- colours ----------
readonly RED=$'\e[31m' GREEN=$'\e[32m' RESET=$'\e[0m'

# ---------- logging ----------
log()  { printf '%b[%s]%b %s\n' "$2" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$RESET" "$1"; }
die()  { log "$*" "$RED"; exit "${2:-1}"; }

# ---------- usage ----------
Usage() {
cat <<EOF
Usage: $0 <build-tag> <version-service>

Environment variables required:
  USERNAME  Jenkins user
  PASSWORD  Jenkins token
  SERVER    Jenkins base URL (https://jenkins.example.com)
EOF
}

# ---------- argument / env check ----------
[[ $# -eq 2 ]] || Usage
[[ -v USERNAME && -n $USERNAME ]] || die "USERNAME not set" 2
[[ -v PASSWORD && -n $PASSWORD ]] || die "PASSWORD not set" 2
[[ -v SERVER   && -n $SERVER   ]] || die "SERVER not set"   2

readonly BUILD_TAG=$1 VERSION_SERVICE=$2
readonly USERPASSWORD="${USERNAME}:${PASSWORD}"

# ---------- temp files ----------
COOKIEJAR=$(mktemp)
trap 'rm -f "$COOKIEJAR"' EXIT INT TERM HUP

# ---------- Jenkins crumb ----------
#CRUMB=$(curl -sS -f -u "$USERPASSWORD" --cookie-jar "$COOKIEJAR" \
CRUMB=$(curl --no-progress-meter -f -u "$USERPASSWORD" --cookie-jar "$COOKIEJAR" \
        "${SERVER}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,':',//crumb)") \
  || die "Failed to fetch Jenkins crumb" 3

# ---------- trigger job ----------
#curl -sS -f -X POST -u "$USERPASSWORD" --cookie "$COOKIEJAR" \
curl --no-progress-meter -f -X POST -u "$USERPASSWORD" --cookie "$COOKIEJAR" \
     -H "${CRUMB:?Missing CRUMB header}"                     \
     -F "BUILD_TAG=${BUILD_TAG}"                             \
     -F "VERSION_SERVICE=${VERSION_SERVICE}"                 \
     "${SERVER}/job/pipelines/job/ops/buildWithParameters"   \
  || die "Canonical Test failed to start for [${BUILD_TAG}] [${VERSION_SERVICE}]" 4

log "Canonical test started for [${BUILD_TAG}] [${VERSION_SERVICE}]" "$GREEN"

exit 0
