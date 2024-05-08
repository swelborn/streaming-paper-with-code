#! /usr/bin/env python

print("electron_count_cori.py")

import argparse
from datetime import datetime

from dateutil import tz

NONE_CORRECTION = "none"
MEAN_CORRECTION = "row-dark-mean"
MEDIAN_CORRECTION = "row-dark-median"

DARKFIELD_CORRECTIONS = [NONE_CORRECTION, MEAN_CORRECTION, MEDIAN_CORRECTION]

parser = argparse.ArgumentParser()
parser.add_argument("--scan_number", "-s", type=int)
parser.add_argument("--distiller_id", "-d", type=int)
parser.add_argument("--threshold", "-t", type=float)
parser.add_argument("--num_threads", "-r", type=int, default=0)
parser.add_argument(
    "--location", "-l", type=str, default="/global/cscratch1/sd/percius"
)
parser.add_argument(
    "--threaded", "-b", type=int, default=1
)  # 1 for threaded, 0 for not
parser.add_argument("--image", "-z", type=str, default="production-stempy")
parser.add_argument("--pad", "-p", action="store_true", default=True)
parser.add_argument(
    "--multi-pass", "-m", dest="multi_pass", action="store_true"
)  # multi-pass testing
parser.add_argument(
    "--timestamp", "-i", type=str, default=datetime.now().isoformat()
)  # New argument for timestamp
args = parser.parse_args()

import json
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

import numpy as np
import stempy.image as stim
import stempy.io as stio
from mpi4py import MPI

tic = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# Inputs
scanNum = args.scan_number
distiller_id = args.distiller_id
th = float(args.threshold)
drive = Path(args.location)
num_threads = args.num_threads
image = args.image
pad = args.pad  # pad scan num with zeros

# Darkfield correction input
apply_row_dark_subtraction = False
apply_row_dark_use_mean = False

# Empty dark reference
dark0 = np.zeros((576, 576))

# Empty gain
gain0 = None

# Format and parse the timestamp
timestamp = datetime.fromisoformat(args.timestamp)
timestamp_local = timestamp.astimezone(tz.gettz("US/Pacific"))
formatted_timestamp = timestamp_local.strftime("%y%m%d_%H%M")

# Setup file name and path
if pad:
    scanName = "data_scan{:010}_*.data".format(scanNum)
else:
    scanName = "data_scan{}_*.data".format(scanNum)

print("Using files in {}".format(drive))
print("scan name = {}".format(scanName))

files = drive.glob(scanName)
iFiles = [str(f) for f in files]

iFiles = sorted(iFiles)

# Electron count the data
sReader = stio.reader(iFiles, stio.FileVersion.VERSION5, backend="multi-pass")


print("start counting #{}".format(scanNum))
t0 = time.time()
electron_counted_data = stim.electron_count(
    sReader,
    dark0,
    gain=gain0,
    number_of_samples=1200,
    verbose=True,
    threshold_num_blocks=20,
    xray_threshold_n_sigma=175,
    background_threshold_n_sigma=th,
    apply_row_dark=apply_row_dark_subtraction,
    apply_row_dark_use_mean=apply_row_dark_use_mean,
)
t1 = time.time()

distiller_id = f"{distiller_id:05}"
scanNum = f"{scanNum:05}"

if rank == 0:
    count_time = t1 - t0
    # as H5 file
    outPath = drive / Path(
        f"FOURD_{formatted_timestamp}_{distiller_id}_{scanNum}_{uuid4()}.h5"
    )
    t0 = time.time()
    stio.save_electron_counts(outPath, electron_counted_data)
    t1 = time.time()
    save_time = t1 - t0
    print(outPath)

    toc = time.time()
    total_time = toc - tic

    # Log performance data to JSON
    performance_data = {
        "timestamp": formatted_timestamp,
        "count_time": count_time,
        "total_time": total_time,
        "save_time": save_time,
        "image": image,
    }

    json_path = drive / ".." / "performance_log.json"
    if json_path.exists():
        with open(json_path, "r") as f:
            current_data = json.load(f)
    else:
        current_data = {}

    if scanNum not in current_data:
        current_data[scanNum] = []
    current_data[scanNum].append(performance_data)

    with open(json_path, "w") as f:
        json.dump(current_data, f, indent=4)
