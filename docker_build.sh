#!/bin/bash

WORKING_DIR=$(dirname "$0")

cd "$WORKING_DIR"
docker build -t samwelborn/streaming-analysis-timing:latest .
