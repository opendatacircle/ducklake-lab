FROM bitnami/spark:3.5.1

USER root
WORKDIR /opt/app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install helpful CLI tools
RUN apt-get update && apt-get install -y \
    less \
    vim \
    iputils-ping \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Duck JDBC
RUN mkdir -p /opt/spark/jars \
 && curl -L https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.3.2.0/duckdb_jdbc-1.3.2.0.jar \
      -o /opt/spark/jars/duckdb_jdbc-1.3.2.0.jar
ENV SPARK_CLASSPATH="/opt/spark/jars/duckdb_jdbc-1.3.2.jar"
# Set PATH for Spark CLI
ENV PATH=$PATH:/opt/bitnami/spark/bin

# Create .duckdb and grant permission for JDBC DuckDB beforehand
RUN mkdir -p /.duckdb
RUN chmod 777 /.duckdb

# Run as root (do not drop privileges)
USER root
