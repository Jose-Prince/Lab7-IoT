from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
from datetime import datetime
import numpy as np

class Consumer:
    def __init__(self, host, port, topic, group_id="weather_station_consumer"):
        self.host = host
        self.port = port
        self.topic = topic
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[f"{host}:{port}"],
            group_id=group_id,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda v: v
        )
        
        # Direcciones del viento (mismo orden que el producer)
        self.directions = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
        
        self.max_points = 50
        self.temperatures = deque(maxlen=self.max_points)
        self.humidities = deque(maxlen=self.max_points)
        self.wind_directions = deque(maxlen=self.max_points)
        self.timestamps = deque(maxlen=self.max_points)
        
        self.message_count = 0
        
    def decode_from_bytes(self, payload):
        """
        Decodifica los 3 bytes recibidos a valores originales.
        
        Byte 1: Temperatura (0-255 -> 0-110.00°C)
        Byte 2: Humedad (0-255 -> 0-100%)
        Byte 3: Índice de dirección del viento (0-7)
        """
        if len(payload) != 3:
            raise ValueError(f"Payload debe ser de 3 bytes, recibido: {len(payload)}")
        
        # Decodificar temperatura
        temp_byte = payload[0]
        temp = (temp_byte / 255.0) * 110.0
        temp = round(temp, 2)
        
        # Decodificar humedad
        hum_byte = payload[1]
        humidity = int((hum_byte / 255.0) * 100.0)
        
        # Decodificar dirección del viento
        dir_idx = payload[2]
        if dir_idx >= len(self.directions):
            dir_idx = 0
        direction = self.directions[dir_idx]
        
        return temp, humidity, direction
    

if __name__ == "__main__":
    import sys
    
    consumer = Consumer("iot.redesuvg.cloud", 9092, "22087")
