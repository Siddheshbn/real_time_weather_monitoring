# from pyspark.sql.functions import when, col

# def generate_weather_alerts(df):
#     """
#     Generates alerts based on specific weather conditions.
#     Adds 'alert' column with alert message or null.
#     """
#     return df.withColumn(
#         "alert",
#         when((col("temperature") > 40), "Extreme heat alert")
#         .when((col("temperature") < 0), "Freezing alert")
#         .when(col("temp_spike") == True, "Sudden temp spike")
#         .otherwise(None)
#     )


def generate_weather_alerts(df):
    return df.filter((df["weather"] == "Rain") | (df["spike"] == 1)).select("city", "timestamp", "weather", "temperature")
