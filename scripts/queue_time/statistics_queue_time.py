import pandas as pd


def calculate_and_write_stats(df, column, filename, section_title):
    """Calculate basic statistics and write to a file."""
    mean_time = df[column].mean()
    median_time = df[column].median()
    std_dev_time = df[column].std()
    min_time = df[column].min()
    max_time = df[column].max()
    quantile_25 = df[column].quantile(0.25)
    quantile_75 = df[column].quantile(0.75)

    stats_str = f"""{section_title}
Mean time: {mean_time} seconds
Median time: {median_time} seconds
Standard Deviation: {std_dev_time} seconds
Minimum time: {min_time} seconds
Maximum time: {max_time} seconds
25th percentile: {quantile_25} seconds
75th percentile: {quantile_75} seconds
--------------------------------------------\n"""

    with open(filename, "a") as f:
        f.write(stats_str)


def main():
    # Read the merged job information file
    df = pd.read_csv("/streaming_analysis/data/file_transfer/merged_job_info.csv")

    # Calculate time between "Submit" and "Start" time
    df["Submit"] = pd.to_datetime(df["Submit"])
    df["Start"] = pd.to_datetime(df["Start"])
    df["queue_time"] = (df["Start"] - df["Submit"]).dt.total_seconds()

    # Remove NaN values for accurate statistics
    df = df.dropna(subset=["queue_time"])

    # Calculate and write statistics with outliers
    calculate_and_write_stats(
        df,
        "queue_time",
        "/streaming_analysis/data/outputs/queue_time_statistics_with_outliers.txt",
        "Statistics with Outliers",
    )

    # Remove outliers based on IQR
    Q1 = df["queue_time"].quantile(0.25)
    Q3 = df["queue_time"].quantile(0.75)
    IQR = Q3 - Q1
    df_filtered = df[
        ~((df["queue_time"] < (Q1 - 1.5 * IQR)) | (df["queue_time"] > (Q3 + 1.5 * IQR)))
    ]

    # Calculate and write statistics without outliers
    calculate_and_write_stats(
        df_filtered,
        "queue_time",
        "/streaming_analysis/data/outputs/queue_time_statistics_without_outliers.txt",
        "Statistics without Outliers",
    )


if __name__ == "__main__":
    main()
