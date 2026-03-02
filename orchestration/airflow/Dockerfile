# Use the official Airflow image
FROM apache/airflow:2.7.1-python3.9

# Switch to root to install system-level dependencies (required for Hive/SASL)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libsasl2-dev \
         libkrb5-dev \
         krb5-multidev \
         gcc \
         openjdk-17-jre-headless \
         procps\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 3. Set JAVA_HOME environment variable so Spark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to the airflow user
USER airflow

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.9.txt"

# Install the Python providers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --constraint "${CONSTRAINT_URL}"