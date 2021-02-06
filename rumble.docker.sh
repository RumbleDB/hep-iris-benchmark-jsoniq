#!/bin/bash

docker run --rm \
    -v $PWD:$PWD \
    rumbledb/rumble:v1.10.0-spark3 \
    "$@"
