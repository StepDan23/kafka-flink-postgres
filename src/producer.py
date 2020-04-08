from numpy.random import choice, randint
from datetime import datetime, timedelta
from time import sleep
from src.utils import connect_kafka_producer


# -------------------------------------------------
# Generation of the dummy data
# -------------------------------------------------
def get_clickstream_entry():
    new_dict = {}

    reaction_list = ["Show", "Click", "Complete", "Like", "Dislike"]
    time_now = datetime.now()
    two_weeks_ago = (time_now - timedelta(days=14)).timestamp()

    new_dict["epk_id"] = randint(10000)
    new_dict["content_id"] = randint(4000)
    new_dict["event_type"] = choice(reaction_list, p=[0.8, 0.1, 0.05, 0.025, 0.025])
    new_dict["event_ts"] = randint(two_weeks_ago, time_now.timestamp())
    new_dict["insert_ts"] = time_now.timestamp()

    return new_dict


if __name__ == "__main__":

    my_server = 'localhost:9092'
    my_topic = 'clickstream'
    producer = connect_kafka_producer(my_server)

    while True:
        for _ in range(20):
            data = get_clickstream_entry()

            try:
                future = producer.send(topic=my_topic, value=data)
                record_metadata = future.get(timeout=10)
                print(f"The message has been sent to a topic: {record_metadata.topic}")

            except Exception as e:
                print(f"Error occurred while sending to {my_topic} topic: {e}")

            finally:
                producer.flush()

        sleep(3)

# TODO: spark_streaming_job.py and container for pyspark