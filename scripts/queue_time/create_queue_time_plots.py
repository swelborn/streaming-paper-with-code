from os import remove

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


def remove_outliers(data):
    """Remove outliers based on IQR."""
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    return data[~((data < (Q1 - 1.5 * IQR)) | (data > (Q3 + 1.5 * IQR)))]


plt.rcParams["font.family"] = "Georgia Pro"


def read_and_prepare_data():
    """Read and prepare data for plotting scatter plots and box plots."""
    df = pd.read_csv("/streaming_analysis/data/file_transfer/merged_job_info.csv")
    df["Submit"] = pd.to_datetime(df["Submit"])
    df["Start"] = pd.to_datetime(df["Start"])
    df["queue_time"] = (df["Start"] - df["Submit"]).dt.total_seconds()
    df = df.dropna(subset=["queue_time"])
    return df


def plot_scatter(df, column, xlabel, filename):
    """Plot a scatter plot for a given column."""
    sns.set_style("ticks")
    sns.scatterplot(
        x=range(len(df)), color="#0762CF", edgecolor="black", y=df[column], s=5
    )
    plt.xlabel("Index")
    plt.ylabel(xlabel)

    _, upper_bound = df[column].quantile([0, 0.90])
    plt.ylim(-5, upper_bound)

    plt.savefig(filename, dpi=600)
    plt.close()


def plot_histogram(df, xlabel, filename, bin_range, num_bins):
    """Plot a histogram for a given size and column."""
    # Define font sizes
    label_font_size = 8
    tick_font_size = 6
    df["Submit"] = pd.to_datetime(df["Submit"])
    df["Start"] = pd.to_datetime(df["Start"])
    df["queue_time"] = (df["Start"] - df["Submit"]).dt.total_seconds()
    df["queue_time"] = remove_outliers(df["queue_time"])

    sns.set_style("ticks")
    plt.figure(figsize=(3, 2))

    # Main plot
    ax_main = sns.histplot(
        df["queue_time"],
        bins=num_bins,
        binrange=bin_range,
        kde=False,
        color="#BB0700",
        edgecolor="black",
        stat="probability",
    )

    plt.xlabel(xlabel, fontsize=label_font_size)
    plt.xlim(-5, bin_range[1])
    plt.ylim(0, 0.1)
    plt.ylabel("Probability", fontsize=label_font_size)
    plt.yticks(np.arange(0, 0.11, step=0.02), fontsize=tick_font_size)
    plt.xticks(np.arange(0, 110, 10), fontsize=tick_font_size)

    # Inset plot
    ax_inset = inset_axes(ax_main, width="40%", height="40%", borderpad=1)
    sns.histplot(
        df["queue_time"],
        bins=num_bins,
        binrange=bin_range,
        kde=False,
        color="#BB0700",
        edgecolor="black",
        stat="probability",
        ax=ax_inset,
    )
    print(f'Average queue time: {df["queue_time"].mean()}')
    print(f'Standard deviation queue time: {df["queue_time"].std()}')
    ax_inset.set_xlim(-5, bin_range[1])
    ax_inset.set_ylim(0, 0.5)

    # Set line width for all four spines
    for axis in ["top", "bottom", "left", "right"]:
        ax_inset.spines[axis].set_linewidth(0.5)
    # Set tick size for both x and y axis
    ax_inset.tick_params(axis="both", which="both", length=1.5, width=0.5)

    plt.xticks(np.arange(0, 110, 20), fontsize=tick_font_size - 1)
    plt.yticks(np.arange(0, 0.6, 0.1), fontsize=tick_font_size - 1)
    plt.ylabel(None)
    plt.xlabel(None)

    # Adjust layout to make sure everything fits
    plt.tight_layout()

    plt.savefig(filename, dpi=600)  # Increase dpi to 600 for higher resolution
    plt.close()


def plot_scatter_by_date(df, column, xlabel, filename):
    """Plot a scatter plot organized by submission date."""
    sns.set_style("ticks")

    # Sort the DataFrame by the 'Submit' column
    df = df.sort_values(by="Submit")

    sns.scatterplot(
        x="Submit", y=column, color="#0762CF", edgecolor="black", data=df, s=5
    )
    plt.xlabel("Submission Date")
    plt.ylabel(xlabel)
    plt.xticks(rotation=45)
    _, upper_bound = df[column].quantile([0, 0.90])
    plt.ylim(-5, upper_bound)

    plt.savefig(filename, dpi=600)
    plt.close()


def main():
    df = read_and_prepare_data()
    plot_scatter(
        df,
        "queue_time",
        "Queue Time (s)",
        "/streaming_analysis/plots/queue_time_scatter.png",
    )
    plot_scatter_by_date(
        df,
        "queue_time",
        "Queue Time (s)",
        "/streaming_analysis/plots/queue_time_scatter_by_date.png",
    )
    plot_histogram(
        df,
        "Queue time (s)",
        "/streaming_analysis/plots/queue_time_hist.png",
        (0, 100),
        100,
    )


if __name__ == "__main__":
    main()
