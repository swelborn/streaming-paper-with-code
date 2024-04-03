import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

plt.rcParams["font.family"] = "Georgia Pro"

filename_map = {
    "128": "streaming_times_128.csv",
    "256": "streaming_times_256.csv",
    "512": "streaming_times_512.csv",
    "1024": "streaming_times_1024.csv",
}


def read_and_prepare_data():
    """Read and prepare data for plotting histograms."""
    df = pd.read_csv("/streaming_analysis/data/file_transfer/merged_job_info.csv")
    offload_df = pd.read_csv(
        "/streaming_analysis/data/file_transfer/ncem_offload_times.csv"
    )
    offload_dict = dict(zip(offload_df["size"], offload_df["offload_time"]))
    streaming_dfs = {
        size: pd.read_csv(f"/streaming_analysis/data/streaming/{filename}")
        for size, filename in filename_map.items()
    }
    return df, offload_dict, streaming_dfs


def plot_histogram(
    df, streaming_df, size, column, xlabel, filename, bin_range, num_bins, offload_time
):
    """Plot a histogram for a given size and column."""
    # Define font sizes
    label_font_size = 8
    tick_font_size = 6

    sns.set_style("ticks")
    filtered_df = df[df["size"] == size]

    # Get the current figure's aspect ratio
    fig = plt.gcf()
    aspect_ratio = fig.get_figheight() / fig.get_figwidth()

    # Set the figure width to 3 inches and adjust the height to maintain the aspect ratio
    plt.figure(figsize=(3, 2))

    # Shift the histogram to the right by adding the offload_time
    shifted_elapsed_time = (
        pd.to_timedelta(filtered_df[column]).dt.total_seconds() + offload_time
    )

    # Main plot
    ax_main = sns.histplot(
        shifted_elapsed_time,
        bins=num_bins,
        binrange=bin_range,
        kde=False,
        color="#BB0700",
        edgecolor="black",
        stat="probability",
    )
    if streaming_df is not None:
        sns.histplot(
            streaming_df["time_difference_seconds"],
            bins=num_bins,
            binrange=bin_range,
            kde=False,
            color="#0762CF",
            edgecolor="black",
            stat="probability",
        )

    plt.xlabel(xlabel, fontsize=label_font_size)
    plt.xlim(-5, bin_range[1])
    plt.ylim(0, 0.2)
    plt.ylabel("Probability", fontsize=label_font_size)
    # plt.title(f"{size}x{size} Job Runtimes", fontsize=label_font_size)
    plt.yticks(np.arange(0, 0.21, step=0.05), fontsize=tick_font_size)
    plt.xticks(np.arange(0, 601, 100), fontsize=tick_font_size)

    # Inset plot
    ax_inset = inset_axes(ax_main, width="40%", height="40%", borderpad=1)
    sns.histplot(
        pd.to_timedelta(filtered_df[column]).dt.total_seconds(),
        bins=num_bins,
        binrange=bin_range,
        kde=False,
        color="#BB0700",
        edgecolor="black",
        stat="probability",
        ax=ax_inset,
    )
    if streaming_df is not None:
        sns.histplot(
            streaming_df["time_difference_seconds"],
            bins=num_bins,
            binrange=bin_range,
            kde=False,
            color="#0762CF",
            edgecolor="black",
            stat="probability",
            ax=ax_inset,
        )
    ax_inset.set_xlim(-10, 600)
    ax_inset.set_ylim(0, 1)

    # Set line width for all four spines
    for axis in ["top", "bottom", "left", "right"]:
        ax_inset.spines[axis].set_linewidth(0.5)
    # Set tick size for both x and y axis
    ax_inset.tick_params(axis="both", which="both", length=1.5, width=0.5)

    plt.xticks(np.arange(0, 601, 200), fontsize=tick_font_size - 1)
    plt.yticks(np.arange(0, 1.1, 0.5), fontsize=tick_font_size - 1)
    plt.ylabel(None)
    plt.xlabel(None)

    # Adjust layout to make sure everything fits
    plt.tight_layout()

    plt.savefig(filename, dpi=600)  # Increase dpi to 600 for higher resolution
    plt.close()


def main():
    df, offload_dict, streaming_dfs = read_and_prepare_data()
    bin_range = (0, 600)
    num_bins = 200
    for size in [128, 256, 512, 1024]:
        size_str = str(size)
        streaming_df = streaming_dfs.get(size_str, None)
        offload_time = offload_dict.get(size, 0)
        plot_histogram(
            df,
            streaming_df,
            size,
            "elapsed",
            "Elapsed Time (s)",
            f"/streaming_analysis/plots/transfer_histogram_{size}.png",
            bin_range,
            num_bins,
            offload_time,
        )
