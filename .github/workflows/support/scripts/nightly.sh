#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ANSI Colors
#
readonly RED=$'\e[31m'
readonly GREEN=$'\e[32m'
readonly RESET=$'\e[0m'

# Logging
#
log()  { printf '%b[%s]%b %s\n' "$2" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$RESET" "$1"; }
die()  { log "$*" "$RED"; exit "${2:-1}"; }

# Usage 
#
Usage() {
cat <<EOF
Usage: $0 <build-tag> <version-service>

Environment variables required:
  USERNAME  Jenkins user
  PASSWORD  Jenkins token
  SERVER    Jenkins base URL (https://jenkins.example.com)
EOF
}

# Preflight Checks
#
[[ $# -eq 2 ]] || Usage
[[ -v USERNAME && -n ${USERNAME} ]] || die "USERNAME not set" 2
[[ -v PASSWORD && -n ${PASSWORD} ]] || die "PASSWORD not set" 2
[[ -v SERVER   && -n ${SERVER}   ]] || die "SERVER not set"   2

readonly BUILD_TAG=${1} 
readonly VERSION_SERVICE=${2}
readonly USERPASSWORD="${USERNAME}:${PASSWORD}"

command -v curl >/dev/null || die "❌ curl is not installed"
command -v mktemp >/dev/null || die "❌ mktemp is not available"

# Jenkins CSRF crumbs
#
COOKIEJAR="$(mktemp -t cookies.XXXXXXXXX)"
NETRC=$(mktemp -t netrc.XXXXXXXXX) || die "Failed to create netrc file" 3
chmod 600 "${NETRC}"
trap 'rm -f "${COOKIEJAR}" "${NETRC}"' EXIT INT TERM HUP

SERVER_HOST=$(awk -F/ '{print $3}' <<<"${SERVER}")
printf "machine %s\nlogin %s\npassword %s\n" "${SERVER_HOST}" "${USERNAME}" "${PASSWORD}" > "${NETRC}"

#CRUMB=$(curl --no-progress-meter -f -u "${USERPASSWORD}" --cookie-jar "${COOKIEJAR}" \
CRUMB=$(curl --no-progress-meter -f --netrc-file "$NETRC" --cookie-jar "${COOKIEJAR}" \
        "${SERVER}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,%22:%22,//crumb)") \
  || die "❌ Error: Failed to fetch Jenkins crumb" 3

# Start Jenkins Job
#
#curl --no-progress-meter -f -X POST -u "${USERPASSWORD}" --cookie "${COOKIEJAR}" \
curl --no-progress-meter -f -X POST --netrc-file "${NETRC}" --cookie "${COOKIEJAR}" \
     -H "${CRUMB:?Missing CRUMB header}"                     \
     -F "BUILD_TAG=${BUILD_TAG}"                             \
     -F "VERSION_SERVICE=${VERSION_SERVICE}"                 \
     "${SERVER}/job/pipelines/job/ops/buildWithParameters"   \
  || die "❌ Error: Canonical Test failed to start for [${BUILD_TAG}] [${VERSION_SERVICE}]" 4

log "✅ Canonical test started for [${BUILD_TAG}] [${VERSION_SERVICE}]" "$GREEN"

exit 0
