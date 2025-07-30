# from pyspark.sql.functions import col
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# def get_top_n_hottest_cities(df, n=5):
#     """
#     Extracts top N hottest cities based on current temperature.
#     Assumes latest temperature per city.
#     """
#     window_spec = Window.orderBy(col("temperature").desc())
#     top_cities_df = df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= n)
#     return top_cities_df.drop("rank")


from pyspark.sql import functions as F

def get_top_n_hottest_cities(df, n=3):
    return df.groupBy("city").agg(F.avg("temperature").alias("avg_temp"))\
             .orderBy(F.desc("avg_temp")).limit(n)
