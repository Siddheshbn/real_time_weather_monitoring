# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# import time
# import requests
# import json
# from confluent_kafka import Producer
# from config.config import OPENWEATHER_API_KEY, KAFKA_CONFIG

# # 1. Get weather data from OpenWeatherMap API
# def get_weather_data(city="Mumbai", units="metric"):
#     url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units={units}"
#     response = requests.get(url)
#     if response.status_code == 200:
#         return response.json()
#     else:
#         print(f"Failed to fetch weather data. Status code: {response.status_code}")
#         return None

# # 2. Format the weather data into a simplified JSON structure
# def format_weather_payload(data):
#     if data is None:
#         return None
#     return {
#         "city": data["name"],
#         "timestamp": data["dt"],
#         "temperature": data["main"]["temp"],
#         "humidity": data["main"]["humidity"],
#         "pressure": data["main"]["pressure"],
#         "weather": data["weather"][0]["description"],
#         "wind_speed": data["wind"]["speed"]
#     }

# # 3. Send formatted data to Kafka
# def send_to_kafka(producer, topic, message):
#     producer.produce(topic, json.dumps(message).encode("utf-8"))
#     producer.flush()
#     print(f"Sent data to Kafka: {message}")

# # 4. Start the weather stream loop
# def start_weather_stream(city="Mumbai", interval=30):
#     producer = Producer(KAFKA_CONFIG)
#     topic = "weather_data"

#     while True:
#         raw_data = get_weather_data(city)
#         formatted_data = format_weather_payload(raw_data)
#         if formatted_data:
#             send_to_kafka(producer, topic, formatted_data)
#         time.sleep(interval)

# # Entry point
# if __name__ == "__main__":
#     start_weather_stream(city="Mumbai", interval=30)


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import requests
from config import config
from producer.kafka_utils import create_producer

def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={config.API_KEY}&units=metric"
    res = requests.get(url)
    return res.json()

def produce_weather():
    producer = create_producer(config.KAFKA_BROKER)
    while True:
        for city in config.CITIES:
            data = fetch_weather(city)
            if data.get("main"):
                payload = {
                    "city": city,
                    "timestamp": data["dt"],
                    "temperature": data["main"]["temp"],
                    "weather": data["weather"][0]["main"]
                }
                print(f'payload:{payload}')
                producer.send(config.KAFKA_TOPIC, payload)
        time.sleep(10)

if __name__ == "__main__":
    produce_weather()