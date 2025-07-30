# from pyspark.sql.functions import when, col

# def categorize_weather(df):
#     """
#     Categorizes weather based on temperature into labels like 'Cold', 'Moderate', 'Hot', 'Extreme'.
#     Adds a new column 'weather_category'.
#     """
#     return df.withColumn(
#         "weather_category",
#         when(col("temperature") < 10, "Cold")
#         .when((col("temperature") >= 10) & (col("temperature") < 25), "Moderate")
#         .when((col("temperature") >= 25) & (col("temperature") < 35), "Hot")
#         .otherwise("Extreme")
#     )

# def categorize_weather(weather):
#     if weather.lower() in ["rain", "drizzle", "thunderstorm"]:
#         return "Wet"
#     elif weather.lower() in ["clear"]:
#         return "Clear"
#     elif weather.lower() in ["clouds"]:
#         return "Cloudy"
#     else:
#         return "Other"

from pyspark.sql.functions import when, col, lower

def categorize_weather(weather_col):
    weather_col = lower(weather_col)
    # Categorize weather based on common weather conditions
    return when(weather_col.isin("rain", "drizzle", "thunderstorm"), "Wet") \
        .when(weather_col.isin("clear"), "Clear") \
        .when(weather_col.isin("clouds"), "Cloudy") \
        .otherwise("Other")
