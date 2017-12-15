#!/bin/sh

source_user=$1
source_password=$2
source_host=$3
source_port=$4
source_schema=$5
target_user=$6
target_password=$7
target_host=$8
target_port=$9
target_schema=${10}

pg_dump --dbname=postgresql://${source_user}:${source_password}@${source_host}:${source_port}/${source_schema} -Fc | pg_restore --dbname=postgresql://${target_user}:${target_password}@${target_host}:${target_port}/${target_schema} -n public -O -1