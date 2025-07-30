# from pyspark.sql.window import Window
# from pyspark.sql.functions import avg, col

# def compute_rolling_avg(df, window_size=3):
#     """
#     Computes rolling average of temperature per city.
#     Adds a column 'rolling_avg_temp'.
#     """
#     window_spec = Window.partitionBy("city").orderBy("timestamp").rowsBetween(-(window_size - 1), 0)
#     return df.withColumn("rolling_avg_temp", avg(col("temperature")).over(window_spec))


from pyspark.sql import functions as F

def compute_rolling_avg(df):
    return df.groupBy("city").agg(F.avg("temperature").alias("avg_temp"))
