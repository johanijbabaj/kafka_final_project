# Use Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /faust_app



# Create the logs directory to avoid FileNotFoundError
RUN mkdir -p /faust_app/logs

# Copy all to the working directory
COPY ./faust_app /faust_app

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r /faust_app/requirements.txt

COPY ./utils/wait-for-it.sh /usr/local/bin/wait-for-it.sh

RUN chmod +x /usr/local/bin/wait-for-it.sh

# Define the command to run Faust
ENTRYPOINT ["wait-for-it.sh", "kafka-0:9092", "--", "faust", "-A", "main", "worker", "-l", "info"]

