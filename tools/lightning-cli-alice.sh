#!/bin/bash
docker exec polar-n1-alice lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest "$@"
