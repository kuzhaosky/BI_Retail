# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.9.3

# Switch to root to install additional packages
USER root

# Install Java (OpenJDK 17), which is required for PySpark
RUN apt-get update && apt-get install -y openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies, including PySpark
RUN pip install --no-cache-dir -r requirements.txt
