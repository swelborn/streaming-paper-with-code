#!/bin/bash

scp mothership:/mnt/nvmedata5/distiller-dev/created_times.csv /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/data/ncem_file_created_times.csv

# 128x128
python /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/extract_file_save_times.py \
    /global/cfs/cdirs/ncemhub/still/counted/2023.10.13 \
    2709 3688

# 256x256
python /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/extract_file_save_times.py \
    /global/cfs/cdirs/ncemhub/still/counted/2023.10.13 \
    3813 4226

# 512x512
python /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/extract_file_save_times.py \
    /global/cfs/cdirs/ncemhub/still/counted/2023.10.13 \
    4227 4644

# 1024x1024
python /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/extract_file_save_times.py \
    /global/cfs/cdirs/ncemhub/still/counted/2023.10.13 \
    4653 4698

# 512x512 with electrons
python /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/extract_file_save_times.py \
    /global/cfs/cdirs/ncemhub/still/counted/2023.10.14 \
    4699 4715