FROM apache/airflow:2.8.1

USER root

# Install Java + wget
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# -----------------------------
# Install Apache Spark
# -----------------------------
RUN wget https://downloads.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# -----------------------------
# Install GCS Connector (Fixes Spark → GCS error)
# -----------------------------
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.15-shaded.jar \
    -O /opt/spark/jars/gcs-connector-hadoop3-2.2.15-shaded.jar

USER airflow