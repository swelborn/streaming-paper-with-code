from pathlib import Path

import pandas as pd


def read_and_append_size(filename, size):
    """Read a CSV file and append a 'size' column."""
    df = pd.read_csv(filename)
    df["size"] = size
    return df


def main():
    data_directory = Path("/streaming_analysis/data/file_transfer")

    # Read the main job information file
    main_df = pd.read_csv(data_directory / "slurm_job_info.csv")

    # Read the additional CSV files and append a 'size' column
    df_128 = read_and_append_size(data_directory / "distiller_db" / "128_128.csv", 128)
    df_256 = read_and_append_size(data_directory / "distiller_db" / "256_256.csv", 256)
    df_512 = read_and_append_size(data_directory / "distiller_db" / "512_512.csv", 512)
    df_1024 = read_and_append_size(
        data_directory / "distiller_db" / "1024_1024.csv", 1024
    )

    # Combine all the additional dataframes into one
    combined_df = pd.concat([df_128, df_256, df_512, df_1024], ignore_index=True)

    # Merge the main dataframe with the combined dataframe based on 'slurm_id'
    merged_df = pd.merge(
        main_df, combined_df, how="left", left_on="Job ID", right_on="slurm_id"
    )

    # Drop the 'slurm_id' column as it's redundant
    merged_df.drop("slurm_id", axis=1, inplace=True)

    # Remove rows where 'id' or 'size' is empty
    merged_df.dropna(subset=["id", "size"], inplace=True)

    # Convert the 'elapsed' column to Timedelta and filter out rows exceeding 29 minutes
    merged_df["elapsed"] = pd.to_timedelta(merged_df["elapsed"])
    merged_df = merged_df[merged_df["elapsed"] <= pd.Timedelta(minutes=29)]

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv(data_directory / "merged_job_info.csv", index=False)


if __name__ == "__main__":
    main()
