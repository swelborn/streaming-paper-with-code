#!/bin/bash

#-- File transfer pre-processing

# python /streaming_analysis/scripts/file_transfer/extract_job_info.py # --> outputs slurm_job_info.csv # Must be done on PM}
python /streaming_analysis/scripts/file_transfer/join_columns_from_db.py # --> outputs /streaming_analysis/data/file_transfer/merged_job_info.py
python /streaming_analysis/scripts/file_transfer/offload_times.py # --> outputs /streaming_analysis/data/file_transfer/ncem_offload_times.csv

#-- Streaming pre-processing
#./file_save_times.sh # --> outputs various scan_times_....csv, must be done on PM. these are provided in data/streaming
python /streaming_analysis/scripts/streaming/join_streaming_data.py # --> outputs /streaming_analysis/data/streaming/f"streaming_times_{key}.csv"

#-- Queue times plots, statistics, ranking of worst days
python /streaming_analysis/scripts/queue_time/create_queue_time_plots.py
python /streaming_analysis/scripts/queue_time/rank_the_worst_days.py
python /streaming_analysis/scripts/queue_time/statistics_queue_time.py

#-- Transfer times histograms and statistics
python /streaming_analysis/scripts/compare/create_transfer_histograms.py
python /streaming_analysis/scripts/compare/statistics_transfer_times.py
python /streaming_analysis/scripts/compare/create_subplot_histograms.py
python /streaming_analysis/scripts/compare/statistics_comparison_table.py