# from pyspark.sql.window import Window
# from pyspark.sql.functions import lag, col, when

# def detect_temp_spikes(df, threshold=5):
#     """
#     Detects temperature spikes greater than the given threshold.
#     Adds a boolean column 'temp_spike'.
#     """
#     window_spec = Window.partitionBy("city").orderBy("timestamp")
#     prev_temp = lag("temperature", 1).over(window_spec)
#     return df.withColumn(
#         "temp_spike",
#         when((col("temperature") - prev_temp) > threshold, True).otherwise(False)
#     )


from pyspark.sql.window import Window
from pyspark.sql import functions as F

def detect_temp_spikes(df):
    window = Window.partitionBy("city").orderBy("timestamp")
    df = df.withColumn("prev_temp", F.lag("temperature").over(window))
    return df.withColumn("spike", F.when((F.col("temperature") - F.col("prev_temp")) > 5, 1).otherwise(0))
