FROM python:3.13-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies (this will install uvicorn and fastapi)
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code from the client_api directory into the container
COPY . /app

# Expose the application port
EXPOSE 8001

# Use uvicorn to serve the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
