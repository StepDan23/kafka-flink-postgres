ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

USER root

#ENV PYSPARK_SUBMIT_ARGS='--packages \
#        org.apache.spark:spark-streaming-kafka-0-8_2.11:$APACHE_SPARK_VERSION pyspark-shell'

USER $NB_USER

WORKDIR /home/$NB_USER/work/

ADD requirements.txt build/

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r build/requirements.txt \
     --ignore-installed PyYAML

