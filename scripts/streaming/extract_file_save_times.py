import csv
import os
import sys
from datetime import datetime


def extract_file_info(input_directory, scan_number_begin, scan_number_end):
    # Initialize a list to store file information
    file_info_list = []

    # Loop through all files in the specified directory
    for filename in os.listdir(input_directory):
        if filename.startswith("FOURD_") and filename.endswith(".h5"):
            # Extract the distiller_id and scan_number from the filename
            parts = filename.split("_")
            distiller_id = parts[-2]
            scan_number = int(parts[-1].split(".")[0])
            if distiller_id == "00000":
                continue

            # Check if the scan_number is within the specified range
            if scan_number_begin <= scan_number <= scan_number_end:
                # Get the last modified time of the file
                file_stat = os.stat(os.path.join(input_directory, filename))
                last_modified_time = datetime.fromtimestamp(
                    file_stat.st_mtime
                ).strftime("%Y-%m-%d %H:%M:%S")

                # Append the distiller_id, scan_number, last_modified_time
                file_info_list.append([distiller_id, scan_number, last_modified_time])

    # Sort the list by distiller_id
    file_info_list.sort(key=lambda x: x[0])

    # Initialize the CSV file with headers
    script_directory = os.path.dirname(os.path.realpath(__file__))
    csv_filename = (
        f"{script_directory}/data/scan_times_{scan_number_begin}_{scan_number_end}.csv"
    )

    with open(csv_filename, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["distiller_id", "scan_number", "datetime"])

        # Write the sorted list to the CSV file
        for row in file_info_list:
            csvwriter.writerow(row)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "Usage: python extract_file_info.py <input_directory> <scan_number_begin> <scan_number_end>"
        )
        sys.exit(1)

    input_directory = sys.argv[1]
    scan_number_begin = int(sys.argv[2])
    scan_number_end = int(sys.argv[3])

    extract_file_info(input_directory, scan_number_begin, scan_number_end)
