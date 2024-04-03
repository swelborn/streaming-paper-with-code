#!/bin/bash

WORKING_DIR=$(dirname "$0")

cd "$WORKING_DIR"

# Convert to real paths and check if directories exist
PLOTS_DIR=$(realpath "${WORKING_DIR}/plots")
OUTPUTS_DIR=$(realpath "${WORKING_DIR}/data/outputs")
SCRIPTS_DIR=$(realpath "${WORKING_DIR}/scripts")
DATA_DIR=$(realpath "${WORKING_DIR}/data")

if [ ! -d "$PLOTS_DIR" ]; then
    echo "PLOTS_DIR does not exist or is not a directory: $PLOTS_DIR"
    exit 1
fi

if [ ! -d "$OUTPUTS_DIR" ]; then
    echo "OUTPUTS_DIR does not exist or is not a directory: $OUTPUTS_DIR"
    exit 1
fi

# Make outputs
docker run --rm -it \
    --mount type=bind,source=${PLOTS_DIR},target=/streaming_analysis/plots \
    --mount type=bind,source=${OUTPUTS_DIR},target=/streaming_analysis/data/outputs \
    --mount type=bind,source=${DATA_DIR},target=/streaming_analysis/data \
    --mount type=bind,source=${SCRIPTS_DIR},target=/streaming_analysis/scripts \
    samwelborn/streaming-analysis-timing:latest /streaming_analysis/scripts/run_all.sh
