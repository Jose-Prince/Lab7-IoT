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
    
    def setup_plots(self):
        """Configura los gráficos en tiempo real."""
        plt.style.use('dark_background')
        self.fig, self.axes = plt.subplots(2, 2, figsize=(14, 10))
        self.fig.suptitle('Estación Meteorológica IoT - Monitoreo en Tiempo Real', 
                         fontsize=16, fontweight='bold')
        
        # Gráfico de temperatura
        self.ax_temp = self.axes[0, 0]
        self.line_temp, = self.ax_temp.plot([], [], 'r-', linewidth=2, marker='o', markersize=4)
        self.ax_temp.set_title('Temperatura (°C)', fontweight='bold')
        self.ax_temp.set_xlabel('Tiempo')
        self.ax_temp.set_ylabel('°C')
        self.ax_temp.grid(True, alpha=0.3)
        self.ax_temp.set_ylim(0, 110)
        
        # Gráfico de humedad
        self.ax_hum = self.axes[0, 1]
        self.line_hum, = self.ax_hum.plot([], [], 'b-', linewidth=2, marker='s', markersize=4)
        self.ax_hum.set_title('Humedad Relativa (%)', fontweight='bold')
        self.ax_hum.set_xlabel('Tiempo')
        self.ax_hum.set_ylabel('%')
        self.ax_hum.grid(True, alpha=0.3)
        self.ax_hum.set_ylim(0, 100)
        
        # Gráfico de dirección del viento (rosa de vientos)
        self.ax_wind = self.axes[1, 0]
        self.wind_counts = {d: 0 for d in self.directions}
        self.bars_wind = self.ax_wind.bar(self.directions, [0]*8, color='green', alpha=0.7)
        self.ax_wind.set_title('Frecuencia de Dirección del Viento', fontweight='bold')
        self.ax_wind.set_xlabel('Dirección')
        self.ax_wind.set_ylabel('Frecuencia')
        self.ax_wind.grid(True, alpha=0.3, axis='y')
        
        # Panel de información
        self.ax_info = self.axes[1, 1]
        self.ax_info.axis('off')
        self.info_text = self.ax_info.text(0.5, 0.5, '', transform=self.ax_info.transAxes,
                                           fontsize=12, verticalalignment='center',
                                           horizontalalignment='center',
                                           family='monospace',
                                           bbox=dict(boxstyle='round', facecolor='#2a2a2a', alpha=0.8))
        self.ax_info.set_title('Información en Tiempo Real', fontweight='bold')
        
        plt.tight_layout()
        
    def update_plots(self, frame):
        """Actualiza los gráficos con los datos más recientes."""
        # Intentar recibir nuevos mensajes (non-blocking)
        messages = self.consumer.poll(timeout_ms=100)
        
        for tp, msgs in messages.items():
            for message in msgs:
                try:
                    temp, hum, direction = self.decode_from_bytes(message.value)
                    
                    # Agregar datos a las colas
                    self.temperatures.append(temp)
                    self.humidities.append(hum)
                    self.wind_directions.append(direction)
                    self.timestamps.append(datetime.now())
                    
                    # Actualizar contador de direcciones
                    self.wind_counts[direction] += 1
                    self.message_count += 1
                    
                    print(f"[{self.message_count}] Recibido -> Temp: {temp}°C, Humedad: {hum}%, Viento: {direction}")
                    
                except Exception as e:
                    print(f"Error decodificando mensaje: {e}")
        
        # Actualizar gráficos si hay datos
        if len(self.temperatures) > 0:
            x_data = list(range(len(self.temperatures)))
            
            # Actualizar línea de temperatura
            self.line_temp.set_data(x_data, list(self.temperatures))
            self.ax_temp.set_xlim(0, max(len(self.temperatures), 10))
            
            # Actualizar línea de humedad
            self.line_hum.set_data(x_data, list(self.humidities))
            self.ax_hum.set_xlim(0, max(len(self.humidities), 10))
            
            # Actualizar barras de dirección del viento
            for bar, direction in zip(self.bars_wind, self.directions):
                bar.set_height(self.wind_counts[direction])
            self.ax_wind.set_ylim(0, max(self.wind_counts.values()) + 1)
            
            # Actualizar panel de información
            if len(self.temperatures) > 0:
                avg_temp = np.mean(list(self.temperatures))
                avg_hum = np.mean(list(self.humidities))
                last_wind = self.wind_directions[-1] if self.wind_directions else "N/A"
                last_time = self.timestamps[-1].strftime("%H:%M:%S") if self.timestamps else "N/A"
                
                # Dirección más frecuente
                most_common_wind = max(self.wind_counts, key=self.wind_counts.get)
                
                info_str = f"""
╔════════════════════════════════╗
║     ÚLTIMOS VALORES            ║
╠════════════════════════════════╣
║ Temperatura: {self.temperatures[-1]:>6.2f} °C       ║
║ Humedad:     {self.humidities[-1]:>6d} %          ║
║ Viento:      {last_wind:>6s}            ║
║ Hora:        {last_time:>8s}          ║
╠════════════════════════════════╣
║     ESTADÍSTICAS               ║
╠════════════════════════════════╣
║ Temp. Promedio: {avg_temp:>6.2f} °C     ║
║ Hum. Promedio:  {avg_hum:>6.1f} %       ║
║ Viento Común:   {most_common_wind:>6s}          ║
║ Mensajes:       {self.message_count:>6d}          ║
╚════════════════════════════════╝
                """
                self.info_text.set_text(info_str)
        
        return self.line_temp, self.line_hum, *self.bars_wind, self.info_text
    
    def start_monitoring(self):
        """Inicia el monitoreo y visualización en tiempo real."""
        print(f"\n{'='*50}")
        print(f"  CONSUMER INICIADO")
        print(f"  Servidor: {self.host}:{self.port}")
        print(f"  Topic: {self.topic}")
        print(f"{'='*50}")
        print("Esperando mensajes del Producer...")
        print("Cierra la ventana de gráficos para detener.\n")
        
        self.setup_plots()
        
        # Animación que actualiza cada 500ms
        ani = animation.FuncAnimation(
            self.fig, 
            self.update_plots, 
            interval=500,
            blit=False,
            cache_frame_data=False
        )
        
        plt.show()
        
    def consume_simple(self):
        """Consumo simple sin gráficos (para pruebas)."""
        print(f"\n{'='*50}")
        print(f"  CONSUMER SIMPLE INICIADO")
        print(f"  Servidor: {self.host}:{self.port}")
        print(f"  Topic: {self.topic}")
        print(f"{'='*50}")
        print("Esperando mensajes... (Ctrl+C para detener)\n")
        
        try:
            for message in self.consumer:
                try:
                    temp, hum, direction = self.decode_from_bytes(message.value)
                    self.message_count += 1
                    
                    print(f"[{self.message_count}] Temp: {temp:>6.2f}°C | Humedad: {hum:>3d}% | Viento: {direction}")
                    
                    # Guardar para estadísticas
                    self.temperatures.append(temp)
                    self.humidities.append(hum)
                    self.wind_directions.append(direction)
                    
                except Exception as e:
                    print(f"Error: {e}")
                    
        except KeyboardInterrupt:
            print("\n\nConsumer detenido.")
            if self.message_count > 0:
                print(f"\nEstadísticas finales:")
                print(f"  Mensajes recibidos: {self.message_count}")
                print(f"  Temp. promedio: {np.mean(list(self.temperatures)):.2f}°C")
                print(f"  Humedad promedio: {np.mean(list(self.humidities)):.1f}%")


if __name__ == "__main__":
    import sys
    
    consumer = Consumer("iot.redesuvg.cloud", 9092, "22087")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--simple":
        consumer.consume_simple()
    else:
        consumer.start_monitoring()