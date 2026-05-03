FROM apache/airflow:2.8.1

USER root

# Java is required for spark-submit client.
RUN apt-get update \
  && apt-get install -y --no-install-recommends curl ca-certificates openjdk-17-jre-headless procps \
  && rm -rf /var/lib/apt/lists/*

# Install a Spark distribution so Airflow can run `spark-submit` and target the remote Spark cluster.
ARG SPARK_VERSION=3.5.1
ARG HADOOP_PROFILE=hadoop3
RUN mkdir -p /opt/spark \
  && for url in \
      "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    ; do \
      echo "Downloading Spark from $url" ; \
      if curl -fL \
        --retry 5 \
        --retry-all-errors \
        --retry-delay 2 \
        --connect-timeout 20 \
        --max-time 900 \
        -o /tmp/spark.tgz \
        "$url" \
      ; then \
        break ; \
      fi ; \
    done \
  && test -s /tmp/spark.tgz \
  && tar -xzf /tmp/spark.tgz --strip-components=1 -C /opt/spark \
  && rm -f /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
  pandas \
  numpy

