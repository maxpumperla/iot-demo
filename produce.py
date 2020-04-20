import time, pickle
from kafka import KafkaProducer


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    with open('json_data.pickle', 'rb') as handle:
        iot_data = pickle.load(handle)
    
    for data in iot_data:
        producer.send("json_data", bytes(data, 'utf-8'))
        # only to make data output more interesting to slow, human observers. kafka can handle without.
        time.sleep(0.2)
