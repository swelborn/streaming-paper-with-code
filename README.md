# Compiling/merging data from various sources

## General notes

- data/
  - contains CSV data for file transfer jobs from the Distiller database, and job information from slurm. These are merged together to form merged_job_info.
  - contains `offload_times.csv`, which contains the averages of offload times.
  - contains streaming information, extracted by calculating the time difference between the first file write time at NCEM and last modified time of the H5 file at NERSC.

## File offload

The offload times were calculated using the creation time stamp from the json files. The "last written" time was extracted from the data files. These were extracted using the following script:

```bash
#!/bin/bash

# Check if the starting scan number is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <starting_scan_number>"
    exit 1
fi

# Initialize the CSV file with headers if it doesn't exist
csv_file="write_times.csv"
if [ ! -f "$csv_file" ]; then
    echo "scan_number,time0,time1,time2,time3,time_last_written" > "$csv_file"
fi

# Loop through all unique scan numbers
for scan_number in $(ls 4dstem_rec_status_*_scan_*.json | grep -o 'scan_[0-9]*' | sort -u | cut -d'_' -f2); do
    # Skip scan numbers less than the starting scan number
    if [ "$scan_number" -lt "$1" ]; then
        continue
    fi

    # Check if the scan number already exists in the CSV file
    if grep -q "^${scan_number}," "$csv_file"; then
        continue
    fi

    # Initialize an empty string to hold the times
    times=""

    # Loop through each receiver number
    for receiver_number in {0..3}; do
        # File name for convenience
        file_name="4dstem_rec_status_${receiver_number}_scan_${scan_number}.json"

        # Extract the time from the JSON file using jq
        raw_time=$(cat $file_name | jq -r '.time')

        # Convert to ISO 8601 format
        iso_time=$(date -d "$raw_time" --utc '+%Y-%m-%dT%H:%M:%S+00:00')

        # Append the time to the string
        times="${times},${iso_time}"
    done

    # Find the most recently modified data file for this scan number
    most_recent_data_file=$(ls -lt data_scan$(printf "%010d" $scan_number)_*.data | head -n 1 | awk '{print $9}')

    # Get the last modified time using stat
    last_written_time=$(stat -c %y "$most_recent_data_file")

    # Convert to ISO 8601 format
    iso_last_written_time=$(date -d "$last_written_time" --utc '+%Y-%m-%dT%H:%M:%S+00:00')

    # Write the scan number, times, and last written time to the CSV file
    echo "${scan_number}${times},${iso_last_written_time}" >> "$csv_file"
done
```

The output csv:

| scan_number | time0                     | time1                     | time2                     | time3                     | time_last_written         |
| ----------- | ------------------------- | ------------------------- | ------------------------- | ------------------------- | ------------------------- |
| 4728        | 2023-10-16T15:30:41+00:00 | 2023-10-16T15:30:39+00:00 | 2023-10-16T15:30:43+00:00 | 2023-10-16T15:30:43+00:00 | 2023-10-16T15:30:45+00:00 |
| 4729        | 2023-10-16T15:50:44+00:00 | 2023-10-16T15:50:42+00:00 | 2023-10-16T15:50:46+00:00 | 2023-10-16T15:50:46+00:00 | 2023-10-16T15:50:48+00:00 |
| ...         | ...                       | ...                       | ...                       | ...                       | ...                       |

This was then performed sequentially, after offloading 30 datasets on each scan size:

- 128:

  - First scan #: 4858
  - Last scan #: 4887
  - Average: 2.7 s

- 256:

  - First scan #: 4828
  - Last scan #: 4857
  - Average: 7.9 s

- 512:
  - First scan #: 4798
  - Last scan #: 4827
  - Average: 33.2
- 1024:
  - First scan #: 4768
  - Last scan #: 4797
  - Average: 149.4 s

See also `data/file_transfer/ncem_offload_times.csv`

## File transfer

These scripts are found in `scripts/file_transfer/`.

### Extracting job information

In order to get the times for counting data for various sizes, `extract_job_info.py` was used. This grabs all of the data in sacct from 2021 to present and puts it into "slurm_job_info.csv"

- Various `continue` controls are put in place to clean data.

```python
if current_job_id in processed_job_ids:
    continue
if "distiller-count" not in job_info["Job Name"]:
    continue
if job_info["Start"] == "Unknown" or job_info["Start"] == "None":
    continue
if job_info["ReqNodes"] != "4":
    continue
if "realtime_" not in job_info["QOS"]:
    continue
if job_info["ExitCode"] != "0:0":
    continue
```

### Extracting data from database

Used rancher to open postgres pod, and ran the following command:

```SQL

\COPY (
    SELECT scans.id, slurm_id, elapsed
    FROM scans
    JOIN jobs ON jobs.scan_id = scans.id
    WHERE (metadata->>'Dimensions.1' = '1024' OR metadata->>'Dimensions 1' = '1024')
      AND (metadata->>'Dimensions.2' = '1024' OR metadata->>'Dimensions 2' = '1024')
) TO '/1024_1024.csv' WITH CSV HEADER;

```

Rancher CLI was then used from perlmutter to copy this file to /data:

`rancher kubectl cp distiller/postgres-b6844b984-shwjz:/1024_1024.csv /pscratch/sd/s/swelborn/streaming-paper/analysis/timing/1024_1024.csv`

Similar commands were run for each size (128x128, 256x256, 512x512, 1024x1024).

The output of the CSV looks like:

| id   | slurm_id | elapsed  |
| ---- | -------- | -------- |
| 1928 | 58246420 | 00:04:35 |
| 1930 | 58250603 | 00:04:19 |
| ...  | ...      | ...      |

### Merging the data

Joining the columns was done using `join_columns_from_db.py`. This gives us `data/file_transfer/merged_job_info.csv`, which contains:

| Job Name             | Job ID  | Timelimit | Elapsed  | Submit              | Start               | End                 | ReqNodes | QOS            | User  | ExitCode | id     | elapsed         | size   |
| -------------------- | ------- | --------- | -------- | ------------------- | ------------------- | ------------------- | -------- | -------------- | ----- | -------- | ------ | --------------- | ------ |
| distiller-count-2320 | 3078555 | 00:30:00  | 00:19:38 | 2022-08-29T17:05:43 | 2022-08-29T17:05:46 | 2022-08-29T17:25:24 | 4        | realtime_m3795 | dstlr | 0:0      | 3288.0 | 0 days 00:19:38 | 1024.0 |
| ...                  | ...     | ...       | ...      | ...                 | ...                 | ...                 | ...      | ...            | ...   | ...      | ...    | ...             | ...    |

## Streaming

These scripts are located in `scripts/streaming/`.

### Experiments

4 experiments:

- 128x128

  - 5 seconds between each acquisition
  - Starting scan number = 2709 (distillerID = 2209)
  - Ending scan number = 3688 (distillerID = 3189)

- 256x256

  - 15 seconds between each acquisition
  - Starting scan number = 3813 (distillerID = 3314)
  - Ending scan number = 4226 (distillerID = 3729)

- 512x512

  - 55 seconds between each acquisition
  - Starting scan number = 4227 (distillerID = 3730)
  - Ending scan number = 4644 (distillerID = 4156)

- 1024x1024
  - 140 seconds between each acquisition
  - Starting scan number = 4653 (distillerID = 3730)
  - Ending scan number = 4698 (distillerID = 4213)
  
### File creation times

Receiver servers were first synced up to internet time using a couple of scripts in remote_control. This one shows the offset:

```bash
#!/bin/bash

time_MS1=$(ssh mothership1 'date +%s%3N')
time_MS6=$(date +%s%3N)
time_offset=$(($time_MS1 - $time_MS6))
echo "Time offset between MS1 and MS6 is: ${time_offset}ms"


time_MS3=$(ssh mothership3 'date +%s%3N')
time_MS6=$(date +%s%3N)
time_offset=$(($time_MS1 - $time_MS6))
echo "Time offset between MS3 and MS6 is: ${time_offset}ms"


time_MS4=$(ssh mothership4 'date +%s%3N')
time_MS6=$(date +%s%3N)
time_offset=$(($time_MS4 - $time_MS6))
echo "Time offset between MS4 and MS6 is: ${time_offset}ms"


time_MS5=$(ssh mothership5 'date +%s%3N')
time_MS6=$(date +%s%3N)
time_offset=$(($time_MS5 - $time_MS6))
echo "Time offset between MS5 and MS6 is: ${time_offset}ms"

```

This was also run at NERSC:

```bash
#!/bin/bash

time_MS6=$(ssh mothership 'date +%s%3N')
time_NERSC=$(date +%s%3N)
time_offset=$(($time_NERSC - $time_MS6))
echo "Time offset between MS6 and NERSC is: ${time_offset}ms"
```

The output was:

```bash
Time offset between MS6 and NERSC is: 4ms
```

This command is used to synchronize (run from the other motherships):

```bash
#!/bin/bash

ssh daquser@mothership6 'date "+%Y-%m-%d %H:%M:%S"' | xargs -I {} sudo date -s "{}"
```

After syncing, the output of the above was:

```bash
Time offset between MS1 and MS6 is: -27ms
Time offset between MS3 and MS6 is: -306ms
Time offset between MS4 and MS6 is: -25ms
Time offset between MS5 and MS6 is: -32ms
```

Then, a script was used to extract the file creation times from the JSON output files. The JSONs are metadata files watched by distiller to see when a scan has been acquired, but they are not data files:

```bash
#!/bin/bash

# Initialize the CSV file with headers
echo "scan_number,time0,time1,time2,time3" > created_times.csv

# Loop through all unique scan numbers
for scan_number in $(ls 4dstem_rec_status_*_scan_*.json | grep -o 'scan_[0-9]*' | sort -u | cut -d'_' -f2); do
    # Initialize an empty string to hold the times
    times=""

    # Loop through each receiver number
    for receiver_number in {0..3}; do
        # Extract the time from the JSON file using jq
        time=$(cat 4dstem_rec_status_${receiver_number}_scan_${scan_number}.json | jq -r '.time')

        # Append the time to the string
        times="${times},${time}"
    done

    # Write the scan number and times to the CSV file
    echo "${scan_number}${times}" >> created_times.csv
done

```

The CSV was double checked before and after the sync process, which shows a good sync of times:

| before / after sync | scan_number | time0                     | time1                     | time2                           | time3                     |
| ------------------- | ----------- | ------------------------- | ------------------------- | ------------------------------- | ------------------------- |
| Before              | 2660        | 2023-10-13T16:25:29+00:00 | 2023-10-13T16:33:54+00:00 | 2023-10-13T16:33:54+00:00       | 2023-10-13T16:33:55+00:00 |
| After               | 2661        | 2023-10-13T16:33:59+00:00 | 2023-10-13T16:34:00+00:00 | 2023-10-13T16:33:59+00:00       | 2023-10-13T16:34:00+00:00 |
| After               | 2662        | 2023-10-13T16:34:04+00:00 | 2023-10-13T16:34:05+00:00 | 2023-10-13T16:34:04+00:00+00:00 | 2023-10-13T16:34:04+00:00 |

### File write times at NERSC

The filesystem time stamp for the write time was used to see when the file was last written to using `extract_file_save_times.py`.

This outputs a file that looks like:

| distiller_id | scan_number | datetime            |
| ------------ | ----------- | ------------------- |
| 02209        | 2709        | 2023-10-13 09:45:14 |
| 02210        | 2710        | 2023-10-13 09:45:18 |
| 02211        | 2711        | 2023-10-13 09:45:23 |
| 02212        | 2712        | 2023-10-13 09:45:28 |
| 02213        | 2713        | 2023-10-13 09:45:33 |

### Merging the data

The above data were merged with `join_streaming_data.py`, leading to various `scan_times_{beginning_scan_number}_{end_scan_number}.`

## Processing

Found in `/scripts`.

- compare directory:

  - Creates plots and statistics outputs for the data.

- queue_time directory:
  - Creates plots for queue time, and statistics.

# Try it out

If you have Docker on your computer, you can create the data using our Dockerfile. We have also provided build/run scripts.

First clone this repository, then navigate to it and run:

```bash
./docker_build.sh
./docker_run.sh
```

This should output plots and outputs in their respective folders.
