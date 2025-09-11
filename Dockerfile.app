FROM python:3.9-slim-bullseye
WORKDIR /app

# Avoid interactive prompts (important for Java install)
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    curl \
    wget \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (better than copying one file at a time)
COPY . .

# Expose ports for app servers
EXPOSE 5001 5002 5003

# Default command
CMD ["python", "app_server.py"]
