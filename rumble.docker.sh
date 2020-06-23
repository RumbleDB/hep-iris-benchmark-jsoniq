#!/bin/bash

docker run --rm \
    -v $PWD:$PWD \
    rumbledb/rumble:v1.8.1-spark3 \
    "$@"
