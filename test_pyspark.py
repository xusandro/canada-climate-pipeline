from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CanadaClimatePipeline")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

spark.range(5).show()

df = spark.createDataFrame(
    [
        ("Toronto", 25.0),
        ("Vancouver", 20.0),
        ("Montreal", 24.0),
    ],
    ["city", "temperature"],
)

df.show()

spark.stop()