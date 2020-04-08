# kafka-spark_streamings-postgres
Here I'm learning how to work with Kafka and write to postgres filtered data
____
Simple steps:

- Building 4 docker containers for project with simple command:

`docker-compose up`

- Launch jupyter notebook from jyp-notebook_kafka:v2 container
- Run cells in Loop_kafka_producer.ipynb (for generate context in Kafka)
- Run cells in Kafka_spark_streams.ipynb (for launch spark streaming job for filtering and inserting to postgres)