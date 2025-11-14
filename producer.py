from kafka import KafkaProducer
import json
import time

class Producer:
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=[f"{host}:{port}"],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_data(self):
        self.producer.send(self.topic, {'message': 'Hello world!'})
        self.producer.flush()

if __name__ == "__main__":
    producer = Producer("iot.redesuvg.cloud", 9092, "22087")

    while(True):
        producer.send_data()
        time.sleep(1)
