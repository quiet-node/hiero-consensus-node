#!/bin/bash
#
set -euo pipefail

# Launch SDCT performance test
Usage() {
    echo "Usage: $0 <user> <password> <server> <build-tag> <version-service>"
    exit 1
}

[[ $# -eq 2 ]] || Usage

BUILD_TAG=${1}
VERSION_SERVICE=${2}

USERNAME=${USERNAME}
PASSWORD=${PASSWORD}
SERVER=${SERVER}

USERPASSWORD="${USERNAME}:${PASSWORD}"

# Set up CRUMB
#
COOKIEJAR="$(mktemp -t cookies.XXXXXXXXX)"
trap 'rm -f "${COOKIEJAR}"' EXIT INT TERM HUP

if !  CRUMB=$(curl --no-progress-meter -f    \
              -u "$USERPASSWORD"             \
              --cookie-jar "${COOKIEJAR}"    \
              "${SERVER}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,%22:%22,//crumb)"); then
    printf '%s ❌ Failed to get CRUMB from Jenkins\n' "$(date '+%Y-%m-%d %T')" >&2
    exit 1
fi

curl_args=(
    --no-progress-meter -f -X POST
    -u "$USERPASSWORD"
    --cookie "${COOKIEJAR}"
    -H "${CRUMB:?Missing CRUMB header}"
    "${SERVER}/job/pipelines/job/ops/buildWithParameters"
    -F "BUILD_TAG=${BUILD_TAG}"
    -F "VERSION_SERVICE=${VERSION_SERVICE}"
)

if ! curl "${curl_args[@]}"; then
    printf '%s ❌ Canonical Test failed to start for [%s][%s]\n' "$(date '+%Y-%m-%d %T')" "${BUILD_TAG}" "${VERSION_SERVICE}" >&2
    exit 1
fi

printf '%s ✅ Canonical Test started for [%s][%s]\n' "$(date '+%Y-%m-%d %T')" "${BUILD_TAG}" "${VERSION_SERVICE}"

exit 0

