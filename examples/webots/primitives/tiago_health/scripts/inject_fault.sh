#!/usr/bin/env bash
# SPDX-License-Identifier: MulanPSL-2.0
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: bash $0 camera|lidar|normal" >&2
    exit 2
fi

sim_container="${ROBONIX_SIM_CONTAINER:-robonix_tiago_sim}"
fault_file="/tmp/robonix-tiago-health-faults.json"

case "$1" in
    camera)
        payload='{"offline":["body/head_camera"]}'
        ;;
    lidar)
        payload='{"offline":["body/hokuyo_lidar"]}'
        ;;
    normal)
        payload='{"offline":[]}'
        ;;
    *)
        echo "unknown fault '$1'; choose camera, lidar, or normal" >&2
        exit 2
        ;;
esac

printf '%s\n' "$payload" |
    docker exec -i "$sim_container" sh -c '
        temporary_file="$1.tmp.$$"
        cat > "$temporary_file"
        mv "$temporary_file" "$1"
    ' sh "$fault_file"

printf '[tiago_health] set injected sensor state: %s\n' "$1"
