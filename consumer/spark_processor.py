# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StringType, DoubleType, LongType
# import json
# import os
# from consumer.processing_utils.categorize_weather import categorize_weather
# from consumer.processing_utils.compute_rolling_avg import compute_rolling_avg
# from consumer.processing_utils.detect_temp_spikes import detect_temp_spikes
# from consumer.processing_utils.generate_weather_alerts import generate_weather_alerts
# from consumer.processing_utils.get_top_n_hottest_cities import get_top_n_hottest_cities

# OUTPUT_DIR = "output"
# os.makedirs(OUTPUT_DIR, exist_ok=True)

# def start_spark_streaming():
#     spark = SparkSession.builder \
#         .appName("RealTimeWeatherMonitoring") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("WARN")

#     schema = StructType() \
#         .add("city", StringType()) \
#         .add("temperature", DoubleType()) \
#         .add("weather", StringType()) \
#         .add("timestamp", LongType())

#     kafka_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "weather") \
#         .load()

#     parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*")

#     # Processed streams
#     categorized = categorize_weather(parsed_df)
#     rolling_avg = compute_rolling_avg(parsed_df)
#     spike_alerts = detect_temp_spikes(parsed_df)
#     top_cities = get_top_n_hottest_cities(parsed_df)
#     alerts = generate_weather_alerts(parsed_df)

#     query1 = categorized.writeStream \
#         .format("json") \
#         .option("path", os.path.join(OUTPUT_DIR, "categorized_weather.json")) \
#         .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk1")) \
#         .outputMode("append") \
#         .start()

#     query2 = rolling_avg.writeStream \
#         .format("json") \
#         .option("path", os.path.join(OUTPUT_DIR, "avg_temp_per_city.json")) \
#         .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk2")) \
#         .outputMode("complete") \
#         .start()

#     query3 = spike_alerts.writeStream \
#         .format("json") \
#         .option("path", os.path.join(OUTPUT_DIR, "spike_alerts.json")) \
#         .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk3")) \
#         .outputMode("append") \
#         .start()

#     query4 = top_cities.writeStream \
#         .format("json") \
#         .option("path", os.path.join(OUTPUT_DIR, "top_hottest_cities.json")) \
#         .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk4")) \
#         .outputMode("complete") \
#         .start()

#     query5 = alerts.writeStream \
#         .format("json") \
#         .option("path", os.path.join(OUTPUT_DIR, "weather_alerts.json")) \
#         .option("checkpointLocation", os.path.join(OUTPUT_DIR, "chk5")) \
#         .outputMode("append") \
#         .start()

#     spark.streams.awaitAnyTermination()

# if __name__ == "__main__":
#     start_spark_streaming()


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, LongType
from config import config
from consumer.processing_utils import categorize_weather, compute_rolling_avg, detect_temp_spikes, generate_weather_alerts, get_top_n_hottest_cities

spark = SparkSession.builder.appName("WeatherConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType()\
    .add("city", StringType())\
    .add("timestamp", LongType())\
    .add("temperature", FloatType())\
    .add("weather", StringType())

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", config.KAFKA_BROKER)\
    .option("subscribe", config.KAFKA_TOPIC)\
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Processing
categorized_df = json_df.withColumn("category", categorize_weather.categorize_weather((col("weather"))))
# rolling_avg_df = compute_rolling_avg.compute_rolling_avg(json_df)
# spike_df = detect_temp_spikes.detect_temp_spikes(json_df)
# alerts_df = generate_weather_alerts.generate_weather_alerts(spike_df)
# top_cities_df = get_top_n_hottest_cities.get_top_n_hottest_cities(json_df)

# Output to JSON
# categorized_df.writeStream.outputMode("append").format("json").option("path", "output/categorized_weather.json").option("checkpointLocation", "chk/cat").start()
# rolling_avg_df.writeStream.outputMode("complete").format("json").option("path", "output/avg_temp_per_city.json").option("checkpointLocation", "chk/avg").start()
# spike_df.writeStream.outputMode("append").format("json").option("path", "output/spike_alerts.json").option("checkpointLocation", "chk/spike").start()
# alerts_df.writeStream.outputMode("append").format("json").option("path", "output/weather_alerts.json").option("checkpointLocation", "chk/alerts").start()
# top_cities_df.writeStream.outputMode("complete").format("json").option("path", "output/top_hottest_cities.json").option("checkpointLocation", "chk/top").start()

categorized_df.writeStream \
.outputMode("append") \
.format("csv") \
.option("path", "output/hashtag_counts_csv") \
.option("checkpointLocation", "output/checkpoint_csv") \
.option("header", "true") \
.start()

query = categorized_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()


query.awaitTermination()


spark.streams.awaitAnyTermination()
