from pathlib import Path

import numpy as np
import pandas as pd


def calculate_data_gb(size):
    _size = int(size)
    total_pixels = _size * _size * 576 * 576
    bit_per_pixel = 16
    byte_per_pixel = bit_per_pixel / 8
    bytes_per_dataset = total_pixels * byte_per_pixel
    gigabytes_per_dataset = bytes_per_dataset * 1e-9
    return gigabytes_per_dataset


def read_statistics(size):
    file_path = Path(
        f"/streaming_analysis/data/outputs/statistics_transfers_{size}.csv"
    )
    df = pd.read_csv(file_path)

    streaming_row = df[df["Statistics"].str.contains("Streaming without Outliers")]
    file_transfer_row = df[df["Statistics"].str.contains("Original without Outliers")]

    streaming_mean = streaming_row["Mean time"].values[0]
    streaming_std = streaming_row["Standard Deviation"].values[0]

    file_transfer_mean = file_transfer_row["Mean time"].values[0]
    file_transfer_std = file_transfer_row["Standard Deviation"].values[0]

    enhancement = file_transfer_mean / streaming_mean
    gigabytes = calculate_data_gb(size)
    return (
        gigabytes,
        file_transfer_mean,
        file_transfer_std,
        streaming_mean,
        streaming_std,
        enhancement,
    )


def main():
    sizes = [128, 256, 512, 1024]
    caption = (
        "Comparison of file transfer and streaming times for various data dimensions."
    )

    latex_table = []

    latex_table.append(r"\begin{table}[H]")
    latex_table.append(r"\caption{" + f"{caption}" + "}")
    latex_table.append(r"\centering")
    latex_table.append(r"\renewcommand{\arraystretch}{1}")
    latex_table.append(r"\begin{tabular}{ccccc}")
    latex_table.append(r"\toprule")
    latex_table.append(
        r"Data Dimension & Data Size (GB) & \makecell{File Transfer (s) \\ ($\mu_{ft} \pm \sigma_{ft}$)} & \makecell{Streaming (s) \\ ($\mu_{s} \pm \sigma_{s}$)} & \makecell{Enhancement \\ ($\mu_{ft}/\mu_{s}$)} \\"
    )
    latex_table.append(r"\midrule")

    for size in sizes:
        (
            gigabytes,
            file_transfer_mean,
            file_transfer_std,
            streaming_mean,
            streaming_std,
            enhancement,
        ) = read_statistics(size)
        latex_table.append(
            f"{size} x {size} x 576 x 576 & {int(gigabytes)} GB & ${file_transfer_mean:.1f} \pm {file_transfer_std:.1f}$ & ${streaming_mean:.1f} \pm {streaming_std:.1f}$ & {enhancement:.1f} \\\\"
        )

    latex_table.append(r"\bottomrule")
    latex_table.append(r"\end{tabular}")
    latex_table.append(r"\label{tab:transfer_count_comparison}")
    latex_table.append(r"\end{table}")

    with open("/streaming_analysis/data/outputs/statistics_table.tex", "w") as f:
        f.write("\n".join(latex_table))


if __name__ == "__main__":
    main()
