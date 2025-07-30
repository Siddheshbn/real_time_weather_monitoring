# Real-Time Weather Monitoring with Kafka & Spark Streaming

This project demonstrates a real-time weather data processing pipeline using:

- Kafka (for streaming data)
- PySpark Streaming (for real-time processing)
- OpenWeatherMap API (for weather data)
- Python (for development)

---

## Folder Structure

real_time_weather_monitoring/
│
├── producer/ # Produces weather data to Kafka
├── consumer/ # Spark streaming jobs
├── consumer/processing_utils/ # Individual processing functions
├── config/ # API keys, topic names, etc.
├── utils/ # Helper functions
├── visualizer/ # Optional dashboard
├── output/ # Output files
├── requirements.txt
└── README.md


---

## Features

1. Weather alerts for extreme conditions  
2. Rolling average temperature per city  
3. Detect sudden temperature spikes  
4. Top-N hottest cities  
5. Categorize weather (hot, cold, etc.)

---

## Setup Instructions

1. Install dependencies  
   ```bash
   pip install -r requirements.txt

    Add your OpenWeatherMap API key in config/config.py

    Start Kafka and Zookeeper

    Run the producer to stream data to Kafka

    Run the Spark consumer to process and save results

Output

Processed JSON files will be saved in the output/ directory.
Requirements

    Python 3.10+

    Kafka & Zookeeper

    OpenWeatherMap API key

# real_time_weather_monitoring
