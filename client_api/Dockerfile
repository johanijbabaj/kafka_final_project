FROM python:3.13-alpine

# Set the working directory inside the container
WORKDIR /app

# Install bash
RUN apk add --no-cache bash

# Copy the requirements file into the container
COPY client_api/requirements.txt .

# Install the dependencies (this will install uvicorn and fastapi)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code from the client_api directory into the container
COPY client_api/. .

COPY utils/wait-for-it.sh /usr/local/bin/wait-for-it.sh
# Make wait-for-it.sh executable
RUN chmod +x /usr/local/bin/wait-for-it.sh

# Expose the application port
EXPOSE 8001

# # Use uvicorn to serve the FastAPI app
CMD ["wait-for-it.sh","elasticsearch:9200", "--timeout=400", "--", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
