FROM python:3.11.6-bullseye

WORKDIR /streaming_analysis
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY data ./data
COPY scripts ./scripts

# Change permissions of all .sh files to be executable
RUN find ./scripts -type f -name "*.sh" -exec chmod +x {} \; && mkdir plots

