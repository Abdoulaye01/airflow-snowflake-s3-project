# Use a specific version for stability
FROM apache/airflow:2.10.5-python3.8

# Set Airflow version environment variable
ENV AIRFLOW_VERSION=2.10.5

# Switch to root user to perform necessary installations
USER root

# Install build-essential and Kerberos libraries (required for compiling dependencies)
RUN apt-get update && apt-get install -y \
    pkg-config \
    libmysqlclient-dev \
    libmariadb-dev \
    build-essential \
    libkrb5-dev \
    krb5-user \
    python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Switch to airflow user to install Python packages
USER airflow


# Copy your custom airflow.cfg to the container with correct ownership
#COPY --chown=airflow:root config/airflow.cfg /opt/airflow/airflow.cfg


# Create a Python virtual environment
RUN python3 -m venv /opt/airflow/venv

# Activate the virtual environment and upgrade pip
RUN /opt/airflow/venv/bin/pip install --no-cache-dir --upgrade pip

# Copy the requirements file into the container
COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt

# Install Python packages inside the virtual environment
RUN /opt/airflow/venv/bin/pip install --no-cache-dir --timeout=300 --retries=5 -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt" \
    -i https://pypi.org/simple/ \
    && rm -f /tmp/requirements.txt

# Set the virtual environment to be used for the container's Python interpreter
ENV PATH="/opt/airflow/venv/bin:$PATH"

# Expose necessary ports (optional if needed for your application)
EXPOSE 8081

# Switch back to non-root user
USER airflow

# Create necessary directories with correct ownership and permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config && \
    chown -R airflow:root /opt/airflow

# Define the command to start Airflow
CMD ["airflow", "webserver"]
