# Use Astronomer Runtime with Airflow pre-installed
FROM astrocrpublic.azurecr.io/runtime:3.1-1

# Switch to root to install system dependencies
USER root

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PySpark and the Airflow Spark provider
RUN pip install pyspark==3.5.1 apache-airflow-providers-apache-spark==4.11.3

# Switch back to non-root user for Airflow
USER astro
