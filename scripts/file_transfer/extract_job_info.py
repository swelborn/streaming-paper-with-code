import csv
import os
import subprocess
from datetime import datetime, timedelta


# Function to run a shell command and return the output
def run_command(command):
    result = subprocess.run(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True
    )
    return result.stdout


# Function to extract job information for multiple job IDs
def extract_multiple_job_info(job_ids):
    job_info_list = []
    job_ids_str = ",".join(job_ids)
    sacct_output = run_command(
        f"sacct --user=dstlr -j {job_ids_str} --format=JobName,JobID,Timelimit,Elapsed,Submit,Start,End,ReqNodes,QOS,User,ExitCode --parsable2 --noheader"
    )
    lines = sacct_output.strip().split("\n")

    processed_job_ids = set()

    for line in lines:
        job_info = {}
        main_line = line.split("|")

        if len(main_line) == 0:
            continue

        current_job_id = main_line[1].split(".")[0]

        # Skip if this job ID has already been processed
        if current_job_id in processed_job_ids:
            continue

        processed_job_ids.add(current_job_id)

        job_info["Job Name"] = main_line[0]
        if "distiller-count" not in job_info["Job Name"]:
            continue

        job_info["Job ID"] = main_line[1]
        job_info["Timelimit"] = main_line[2]
        job_info["Elapsed"] = main_line[3]
        job_info["Submit"] = main_line[4]
        job_info["Start"] = main_line[5]
        if job_info["Start"] == "Unknown" or job_info["Start"] == "None":
            continue
        job_info["End"] = main_line[6]
        job_info["ReqNodes"] = main_line[7]
        if job_info["ReqNodes"] != "4":
            continue
        job_info["QOS"] = main_line[8]
        if "realtime_" not in job_info["QOS"]:
            continue
        job_info["User"] = main_line[9]
        job_info["ExitCode"] = main_line[10]
        if job_info["ExitCode"] != "0:0":
            continue

        job_info_list.append(job_info)

    return job_info_list


# Function to get job IDs within smaller date ranges
def get_all_job_ids(start_date, end_date, delta_days=30):
    job_ids = []
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    final_end = datetime.strptime(end_date, "%Y-%m-%d")

    while current_start <= final_end:
        current_end = current_start + timedelta(days=delta_days)
        if current_end > final_end:
            current_end = final_end

        job_ids_output = run_command(
            f"sacct --user=dstlr --format=JobID --noheader --starttime={current_start.strftime('%Y-%m-%d')} --endtime={current_end.strftime('%Y-%m-%d')}"
        )
        current_job_ids = job_ids_output.strip().split("\n")

        for job_id in current_job_ids:
            split_job_id = job_id.split()
            if len(split_job_id) > 0 and not "." in split_job_id[0]:
                job_ids.append(split_job_id[0])

        current_start = current_end + timedelta(days=1)

    return job_ids


# Main function to write job information to CSV
def main():
    start_date = "2021-01-01"
    end_date = "2023-10-12"

    job_ids = get_all_job_ids(start_date, end_date)
    script_directory = os.path.dirname(os.path.abspath(__file__))
    output_file_path = os.path.join(script_directory, "/data/slurm_job_info.csv")

    with open(output_file_path, "w", newline="") as csvfile:
        fieldnames = [
            "Job Name",
            "Job ID",
            "Timelimit",
            "Elapsed",
            "Submit",
            "Start",
            "End",
            "ReqNodes",
            "QOS",
            "User",
            "ExitCode",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        # Fetch job info in chunks to avoid overloading sacct
        chunk_size = 100
        for i in range(0, len(job_ids), chunk_size):
            job_info_list = extract_multiple_job_info(job_ids[i : i + chunk_size])
            for job_info in job_info_list:
                writer.writerow(job_info)


if __name__ == "__main__":
    main()
