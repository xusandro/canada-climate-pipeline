from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PATH = "data/processed/observations.parquet"

spark = (
    SparkSession.builder
    .appName("analysis-after-glue")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(PATH)

yearly = (
    df.groupBy("year")
    .count()
    .orderBy("year")
)

window = Window.orderBy("year")

result = (
    yearly
    .withColumn(
        "previous_year_count",
        F.lag("count").over(window)
    )
    .withColumn(
        "change_pct",
        F.round(
            (F.col("count") - F.col("previous_year_count"))
            / F.col("previous_year_count")
            * 100,
            2,
        )
    )
)

monthly2024 = df.filter(F.col("year") == 2024).groupBy("month").count().orderBy("month")
tmax2024 = df.filter(F.col("year") == 2024).filter(F.col("tmax").isNotNull()).groupBy("month").count().orderBy("month")

tmax2024.show(12, truncate=False)
monthly2024.show(12, truncate=False)

result.show(100, truncate=False)

spark.stop()


# Noted a big drop in the number of observations in 2024, specifically starting from May
# also 2024 and 2025 observations counts dropped significantly compared to previous years.
# With 37 percent drop in 2024 compared to 2023 