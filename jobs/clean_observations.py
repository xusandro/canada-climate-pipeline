from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


OBSERVATION_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable = False),
        StructField("date", StringType(), nullable = False),
        StructField("element", StringType(), nullable = False),
        StructField("data_value", StringType(), nullable = True),
        StructField("m_flag", StringType(), nullable = True),
        StructField("q_flag", StringType(), nullable = True),
        StructField("s_flag", StringType(), nullable = True),
        StructField("obs_time", StringType(), nullable = True)

    ]
)


CANADA = "CA"


#	   The five core elements according to readme are:
# PRCP = Precipitation (tenths of mm)
# SNOW = Snowfall (mm)
# SNWD = Snow depth (mm)
# TMAX = Maximum temperature (tenths of degrees C)
# TMIN = Minimum temperature (tenths of degrees C)

CORE_ELEMENTS = ["TMAX", "TMIN", "PRCP", "TAVG", "SNOW", "SNWD"]

path = "data/raw/2024.csv.gz"



spark = (
        SparkSession.builder.appName("initial_spark_test")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("ERROR")


df = spark.read.csv(path, schema=OBSERVATION_SCHEMA, header=False)
print(df.filter(F.substring(F.col('id'), 1, 2) == CANADA).count())
