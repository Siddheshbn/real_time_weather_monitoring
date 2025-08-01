# Real-Time Weather Monitoring with Kafka & Spark Streaming

This project demonstrates a real-time weather data processing pipeline using:

- Kafka (for streaming data)
- PySpark Streaming (for real-time processing)
- OpenWeatherMap API (for weather data)
- Python (for development)

---

## Folder Structure

real_time_weather_monitoring/ <br>
│ <br>
├── producer/ # Produces weather data to Kafka  <br>
├── consumer/ # Spark streaming jobs <br>
├── consumer/processing_utils/ # Individual processing functions <br>
├── config/ # API keys, topic names, etc. <br>
├── utils/ # Helper functions <br>
├── visualizer/ # Optional dashboard <br>
├── output/ # Output files <br>
├── requirements.txt <br>
└── README.md <br>


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

command to run the weather app :-
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  /home/<username>/projects/real_time_weather_monitoring/consumer/spark_processor.py

