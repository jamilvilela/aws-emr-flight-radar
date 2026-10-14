#!/bin/bash
# Bootstrap script to install Delta Lake JARs on EMR cluster
# This script runs on all nodes before the cluster starts

set -e

echo "Installing Delta Lake JARs..."

# EMR 7.5.0 with Spark 3.5.0 uses Delta Lake 3.1.0
DELTA_VERSION="3.1.0"
SPARK_VERSION="2.13"
DELTA_JAR="delta-spark_${SPARK_VERSION}-${DELTA_VERSION}.jar"
DELTA_STORAGE_JAR="delta-storage-${DELTA_VERSION}.jar"

# Target directory for JARs
JAR_DIR="/usr/lib/spark/jars"

# Download Delta Lake JARs from Maven Central
echo "Downloading Delta Lake JARs..."
cd /tmp

# Download delta-spark JAR
wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/3.1.0/delta-spark_2.13-3.1.0.jar" -O "/tmp/delta-spark_2.13-3.1.0.jar"

# Download delta-storage JAR
wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar" -O "/tmp/delta-storage-3.1.0.jar"

# Move JARs to target directory with sudo
sudo mv "/tmp/delta-spark_2.13-3.1.0.jar" "${JAR_DIR}/delta-spark_2.13-3.1.0.jar"
sudo mv "/tmp/delta-storage-3.1.0.jar" "${JAR_DIR}/delta-storage-3.1.0.jar"

# Verify JARs were installed
if [ -f "${JAR_DIR}/delta-spark_2.13-3.1.0.jar" ] && [ -f "${JAR_DIR}/delta-storage-3.1.0.jar" ]; then
    echo "Delta Lake JARs installed successfully"
    ls -la "${JAR_DIR}/delta-spark_2.13-3.1.0.jar" "${JAR_DIR}/delta-storage-3.1.0.jar"
else
    echo "ERROR: Failed to install Delta Lake JARs"
    exit 1
fi

echo "Delta Lake JARs installation complete"