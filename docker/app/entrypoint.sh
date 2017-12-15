#!/bin/bash

. ./capture_error.sh
EXEC_JAR="/usr/bin/java -Xms256m -Xmx512m -jar /app/service.jar"
case "${1}" in
    bash)
        shift
        exec /bin/bash "${@}"
        ;;
    check)
        exec ${EXEC_JAR} check /app/config.yaml
        ;;
    server)
        run_cmd 'Failed to start service' exec ${EXEC_JAR} server /app/config.yaml
        ;;
    *)
        exec ${EXEC_JAR} "${@}"
        ;;
esac
