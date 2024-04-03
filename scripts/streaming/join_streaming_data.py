from pathlib import Path

import pandas as pd
import pytz

filename_map = {
    "128": {
        "filename": "scan_times_2709_3688.csv",
        "initial_scan_number": 2709,
        "final_scan_number": 3688,
    },
    "256": {
        "filename": "scan_times_3813_4226.csv",
        "initial_scan_number": 3813,
        "final_scan_number": 4226,
    },
    "512": {
        "filename": "scan_times_4227_4644.csv",
        "initial_scan_number": 4227,
        "final_scan_number": 4644,
    },
    "1024": {
        "filename": "scan_times_4653_4698.csv",
        "initial_scan_number": 4653,
        "final_scan_number": 4698,
    },
}


def main():
    # Define the script directory
    base_path = Path("/streaming_analysis/data/")

    # Iterate through all entries in filename_map
    for key, value in filename_map.items():
        scan_times_filename = value["filename"]
        scan_times_path = base_path / "streaming" / scan_times_filename
        ncem_file_created_times_path = (
            base_path / "streaming" / "ncem_file_created_times.csv"
        )

        # Read the CSV files into DataFrames
        df_scan_times = pd.read_csv(scan_times_path)
        df_ncem_file_created_times = pd.read_csv(ncem_file_created_times_path)

        # Merge the DataFrames on the 'scan_number' column
        df_merged = pd.merge(
            df_scan_times, df_ncem_file_created_times, on="scan_number", how="inner"
        )

        # Rename the 'datetime' column to 'nersc_write_time'
        df_merged.rename(columns={"datetime": "nersc_write_time"}, inplace=True)

        # Convert the time columns to datetime format for comparison
        df_merged["nersc_write_time"] = pd.to_datetime(df_merged["nersc_write_time"])
        for col in ["time0", "time1", "time2", "time3"]:
            df_merged[col] = pd.to_datetime(df_merged[col])

        # Select the lowest of the four times and create a new column
        df_merged["ncem_created_time"] = df_merged[
            ["time0", "time1", "time2", "time3"]
        ].min(axis=1)

        # Drop the now-redundant time columns
        df_merged.drop(["time0", "time1", "time2", "time3"], axis=1, inplace=True)

        # Adjust the time zone for 'ncem_created_time' to match 'nersc_write_time'
        utc = pytz.UTC
        df_merged["ncem_created_time"] = df_merged["ncem_created_time"].dt.tz_convert(
            utc
        ).dt.tz_localize(None) - pd.Timedelta(hours=7)

        # Calculate the time difference between 'nersc_write_time' and 'ncem_created_time' in seconds
        df_merged["time_difference_seconds"] = (
            (df_merged["ncem_created_time"] - df_merged["nersc_write_time"])
            .dt.total_seconds()
            .abs()
        )

        # Save the final DataFrame to a new CSV file
        output_filename = f"streaming_times_{key}.csv"
        output_path = base_path / "streaming" / output_filename
        df_merged.to_csv(output_path, index=False)


if __name__ == "__main__":
    main()
