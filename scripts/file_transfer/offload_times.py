from datetime import datetime
from pathlib import Path

import pandas as pd

# Define the scan number ranges for each size
start_and_end_scan_numbers = {
    "128": {"start": 4858, "end": 4887},
    "256": {"start": 4828, "end": 4857},
    "512": {"start": 4798, "end": 4827},
    "1024": {"start": 4768, "end": 4797},
}


def calculate_offload_time(row):
    # Parse the datetime strings for all four times and the last written time
    times = [
        datetime.fromisoformat(row[f"time{i}"].replace("Z", "+00:00")) for i in range(4)
    ]
    time_last_written = datetime.fromisoformat(
        row["time_last_written"].replace("Z", "+00:00")
    )

    # Sort the times and select the earliest
    earliest_time = min(times)

    # Calculate the time difference in seconds
    return (time_last_written - earliest_time).total_seconds()


def main():
    data_directory = Path("/streaming_analysis/data/file_transfer")

    # Read the main job information file
    write_times_df = pd.read_csv(data_directory / "write_times.csv")

    # Calculate the offload time for each row
    write_times_df["offload_time"] = write_times_df.apply(
        calculate_offload_time, axis=1
    )

    # Initialize a list to store the average offload times
    avg_offload_times_list = []

    # Loop through each size and its corresponding scan number range
    for size, scan_range in start_and_end_scan_numbers.items():
        # Filter the DataFrame based on the scan number range
        filtered_df = write_times_df[
            (write_times_df["scan_number"] >= scan_range["start"])
            & (write_times_df["scan_number"] <= scan_range["end"])
        ]

        # Calculate the average offload time for this size
        avg_offload_time = filtered_df["offload_time"].mean()

        # Append the result to the list
        avg_offload_times_list.append({"size": size, "offload_time": avg_offload_time})

    # Convert the list to a DataFrame
    avg_offload_times_df = pd.DataFrame(avg_offload_times_list)

    # Save the average offload times to a CSV file
    avg_offload_times_df.to_csv(data_directory / "ncem_offload_times.csv", index=False)


if __name__ == "__main__":
    main()
