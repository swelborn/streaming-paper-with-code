import pandas as pd


def main():
    # Read the merged job information file
    df = pd.read_csv("/streaming_analysis/data/file_transfer/merged_job_info.csv")

    # Convert 'Submit' and 'Start' columns to datetime objects
    df["Submit"] = pd.to_datetime(df["Submit"])
    df["Start"] = pd.to_datetime(df["Start"])

    # Calculate time between "Submit" and "Start" in seconds
    df["queue_time"] = (df["Start"] - df["Submit"]).dt.total_seconds()

    # Remove NaN values for accurate calculations
    df = df.dropna(subset=["queue_time"])

    # Extract the date from the 'Submit' datetime object
    df["Submit_date"] = df["Submit"].dt.date

    # Group by the 'Submit_date' and calculate the mean time between "Submit" and "Start"
    grouped_df = df.groupby("Submit_date")["queue_time"].mean().reset_index()

    # Sort the DataFrame based on the mean time between "Submit" and "Start"
    sorted_df = grouped_df.sort_values("queue_time", ascending=False)

    # Write the sorted DataFrame to a CSV file
    sorted_df.to_csv("/streaming_analysis/data/outputs/ranked_days.csv", index=False)

    # Extract the date and month-year from the 'Submit' datetime object
    df["Submit_month_year"] = df["Submit"].dt.to_period("M")

    # Group by the 'Submit_date' and 'Submit_month_year', then calculate the mean time between "Submit" and "Start"
    grouped_df = (
        df.groupby(["Submit_month_year", "Submit_date"])["queue_time"]
        .mean()
        .reset_index()
    )

    # Sort the DataFrame based on the mean time between "Submit" and "Start" within each month
    sorted_df = grouped_df.sort_values(
        ["Submit_month_year", "queue_time"], ascending=[True, True]
    )

    # Write the sorted DataFrame to a CSV file
    sorted_df.to_csv(
        "/streaming_analysis/data/outputs/ranked_days_by_month.csv", index=False
    )


if __name__ == "__main__":
    main()
