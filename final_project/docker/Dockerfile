FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY scripts/requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      apache-airflow==2.10.5 pyspark==3.5.4 boto3==1.36.3
# Airflow constraints pin an older confluent-kafka than the replay script uses.
RUN pip install --no-cache-dir confluent-kafka==2.15.0 jupyterlab==4.3.5

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
WORKDIR /opt/airflow
