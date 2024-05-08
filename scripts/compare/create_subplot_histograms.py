import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from mpl_toolkits.axes_grid1.inset_locator import inset_axes

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


def plot_subplot(
    ax, df, streaming_df, size, column, bin_range, num_bins, offload_time, write_time
):
    """Plot a histogram on a given Matplotlib axis."""
    # Define font sizes
    label_font_size = 8
    tick_font_size = 6

    # Set Seaborn style
    sns.set_style("ticks")

    # Filter DataFrame based on size
    filtered_df = df[df["size"] == size]

    # Shift the histogram to the right by adding the offload_time
    shifted_elapsed_time = (
        pd.to_timedelta(filtered_df[column]).dt.total_seconds()
        + offload_time
        - write_time
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
        ax=ax,
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
            ax=ax,
        )

    ax.set_xlim(-5, bin_range[1])
    ax.set_ylim(0, 0.2)
    ax.set_ylabel("Probability", fontsize=label_font_size)
    ax.set_yticks(np.arange(0, 0.21, step=0.05))
    ax.tick_params(axis="y", labelsize=tick_font_size)
    ax.tick_params(axis="x", labelsize=tick_font_size)

    # Inset plot
    ax_inset = inset_axes(ax, width="40%", height="40%", borderpad=1)
    sns.histplot(
        shifted_elapsed_time,
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

    ax_inset.set_xticks(np.arange(0, 601, 200))
    ax_inset.tick_params(axis="x", labelsize=tick_font_size - 1)
    ax_inset.set_yticks(np.arange(0, 1.1, 0.5))
    ax_inset.tick_params(axis="y", labelsize=tick_font_size - 1)
    ax_inset.set_ylabel(None)
    ax_inset.set_xlabel(None)


def main():
    df, offload_dict, streaming_dfs, write_time_df = read_and_prepare_data()
    bin_range = (0, 600)
    num_bins = 200

    fig, axes = plt.subplots(4, 1, figsize=(3, 7), sharex=True)
    plt.subplots_adjust(hspace=0.05)

    for ax, size in zip(axes, [128, 256, 512, 1024]):
        size_str = str(size)
        streaming_df = streaming_dfs.get(size_str, None)
        offload_time = offload_dict.get(size, 0)
        write_time = write_time_df.get(size)
        plot_subplot(
            ax,
            df,
            streaming_df,
            size,
            "elapsed",
            bin_range,
            num_bins,
            offload_time,
            write_time,
        )

    axes[-1].set_xlabel("Transfer and Count Time (s)", fontsize=8)

    plt.tight_layout()
    plt.savefig("/streaming_analysis/plots/transfer_histogram_combined.png", dpi=600)
    plt.close()


if __name__ == "__main__":
    main()
