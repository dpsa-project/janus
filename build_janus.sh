#!/bin/sh

function build
{
    DOCKER_BUILDKIT=1 docker build . --build-arg BINARY=$1 -t janus_$1
}

build "aggregator"
build "collection_job_driver"
build "aggregation_job_driver"
build "aggregation_job_creator"
