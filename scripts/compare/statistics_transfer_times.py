from pathlib import Path

import pandas as pd

filename_map = {
    "128": "streaming_times_128.csv",
    "256": "streaming_times_256.csv",
    "512": "streaming_times_512.csv",
    "1024": "streaming_times_1024.csv",
}


def remove_outliers(data):
    """Remove outliers based on IQR."""
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    return data[~((data < (Q1 - 1.5 * IQR)) | (data > (Q3 + 1.5 * IQR)))]


def calculate_and_write_stats(df, column, stats_dict, size, section_title):
    """Calculate basic statistics and append to a dictionary."""
    if column is not None:
        data = df[column]
    else:
        data = df  # If column is None, df is actually a Series

    n_points = len(data)
    mean_time = data.mean()
    median_time = data.median()
    std_dev_time = data.std()
    min_time = data.min()
    max_time = data.max()
    quantile_25 = data.quantile(0.25)
    quantile_75 = data.quantile(0.75)

    stats_dict[section_title] = {
        "Size": size,
        "Number of Points": n_points,
        "Mean time": mean_time,
        "Median time": median_time,
        "Standard Deviation": std_dev_time,
        "Minimum time": min_time,
        "Maximum time": max_time,
        "25th percentile": quantile_25,
        "75th percentile": quantile_75,
    }


def read_and_prepare_data():
    """Read and prepare data for plotting histograms."""
    df = pd.read_csv("/streaming_analysis/data/file_transfer/merged_job_info.csv")
    offload_df = pd.read_csv(
        "/streaming_analysis/data/file_transfer/ncem_offload_times.csv"
    )
    offload_dict = dict(zip(offload_df["size"], offload_df["offload_time"]))

    # Make write times
    write_time_df = pd.read_csv(
        "/streaming_analysis/data/file_transfer/save_time_stats.csv"
    )
    # Get the mean times for everything
    mean_df = write_time_df[write_time_df["Stat"] == "mean"]

    # Get the save times
    pivot_df = mean_df.pivot(index="Size", columns="Type", values="save_time")

    # subtract blank time
    pivot_df["overhead"] = pivot_df["Real"] - pivot_df["Blank"]

    # just get the overheads in a dictionary.
    save_overhead = pivot_df["overhead"].to_dict()

    streaming_dfs = {
        size: pd.read_csv(f"/streaming_analysis/data/streaming/{filename}")
        for size, filename in filename_map.items()
    }
    return df, offload_dict, streaming_dfs, save_overhead


def main():
    merged_df, offload_dict, streaming_dfs, save_overhead = read_and_prepare_data()

    for size, _ in filename_map.items():
        stats_dict = {}

        # Calculate statistics for Streaming histograms with outliers
        calculate_and_write_stats(
            streaming_dfs[size],
            "time_difference_seconds",
            stats_dict,
            size,
            f"Statistics for {size}x{size} Streaming with Outliers",
        )

        # Calculate statistics for Streaming histograms without outliers
        filtered_streaming_data = remove_outliers(
            streaming_dfs[size]["time_difference_seconds"]
        )
        calculate_and_write_stats(
            filtered_streaming_data,
            None,
            stats_dict,
            size,
            f"Statistics for {size}x{size} Streaming without Outliers",
        )

        # Filter the merged_df for the current size for Original histograms
        filtered_merged_df = merged_df[merged_df["size"] == float(size)]
        elapsed_seconds = pd.to_timedelta(
            filtered_merged_df["Elapsed"]
        ).dt.total_seconds()

        # Add offload time to Original histograms
        offload_time = offload_dict.get(int(size), 0)
        elapsed_seconds += offload_time
        elapsed_seconds -= save_overhead.get(int(size), 0) * 2  # Subtract 2x overhead

        # Calculate statistics for Original histograms with outliers
        calculate_and_write_stats(
            elapsed_seconds,
            None,
            stats_dict,
            size,
            f"Statistics for {size}x{size} Original with Outliers",
        )

        # Calculate statistics for Original histograms without outliers
        filtered_original_data = remove_outliers(elapsed_seconds)
        calculate_and_write_stats(
            filtered_original_data,
            None,
            stats_dict,
            size,
            f"Statistics for {size}x{size} Original without Outliers",
        )

        # Write all statistics to a single CSV file for each size
        stats_df = pd.DataFrame.from_dict(stats_dict, orient="index")
        stats_df.index.name = "Statistics"
        stats_df.to_csv(
            Path("/streaming_analysis/data/outputs")
            / f"statistics_transfers_{size}.csv"
        )


if __name__ == "__main__":
    main()
