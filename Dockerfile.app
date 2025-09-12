# Use an up-to-date Python base on Debian Bookworm
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies with security patches
RUN apt-get update && apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        curl \
        wget \
    && apt-get autoremove -y && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Hadoop/Java clients
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy Python dependencies file first (cache layer)
COPY requirements.txt .

# Upgrade pip & install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose ports for Flask servers
EXPOSE 5001 5002 5003

# Run the application server
CMD ["python", "app_server.py"]
