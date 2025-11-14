from kafka import KafkaProducer
import numpy as np
import json
import time

class Producer:
    def __init__(self, host, port, topic):
        self.host = host
        self.port = port
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=[f"{host}:{port}"],
            value_serializer=lambda v: v
        )

    def generate_data(self):
        directions = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

        # Temperature
        temp = np.random.normal(loc=55.0, scale=15.0)
        temp = np.clip(temp, 0.0, 110.0) # 0 - 110.00
        temp = round(temp, 2)

        # Humidity
        humidity = np.random.normal(loc=50.0, scale=20.0)
        humidity = int(np.clip(humidity, 0.0, 100.0)) # 0 - 100

        # Direction
        idx = np.random.normal(loc=len(directions)/2, scale=2.0)
        idx = int(np.clip(round(idx), 0, len(directions) - 1))
        direction = directions[idx]

        return float(temp), humidity, direction, idx

    def encode_to_bytes(self, temp, hum, dir_idx):
        # Byte 1
        temp_byte = int((temp / 110.0) * 255) & 0xFF

        # Byte 2 
        hum_byte = int((hum / 100.0) * 255) & 0xFF

        # Byte 3
        dir_byte = dir_idx & 0xFF

        return bytes([temp_byte, hum_byte, dir_byte])

    def send_data(self):
        temp, humd, direc, dir_idx = self.generate_data()

        payload = self.encode_to_bytes(temp, humd, dir_idx)

        self.producer.send(self.topic, payload)
        print(f'\nTemperature: {temp}, Humidity: {humd}%, Direction: {direc}')
        self.producer.flush()

if __name__ == "__main__":
    producer = Producer("iot.redesuvg.cloud", 9092, "22087")

    while(True):
        producer.send_data()
        time.sleep(1)
