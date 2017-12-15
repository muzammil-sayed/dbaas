#!/bin/bash

# An error exit function

function error_exit {
	echo "$1" 1>&2
	exit 1
}

run_cmd() {
    local e r m=$1
    shift
    exec 6>&1
    e=$("$@" 2>&1 >&6)
    r=$?
    exec 6>&-
    ((r)) || return 0
    printf '{"service_environment":"%s","service_name":"%s","service_pipeline":"%s","service_version":"%s","Message":"%s"}\n' "$MAKO_ENVIRONMENT" "$MAKO_SERVICE_ID" "$MAKO_PIPELINE" "$MAKO_VERSION" "$e"
    echo "$e" > /dev/termination-log
    error_exit "$m" "$e"
}