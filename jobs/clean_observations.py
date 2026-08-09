from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

OBSERVATION_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("element", StringType(), nullable=False),
        StructField("data_value", StringType(), nullable=True),
        StructField("m_flag", StringType(), nullable=True),
        StructField("q_flag", StringType(), nullable=True),
        StructField("s_flag", StringType(), nullable=True),
        StructField("obs_time", StringType(), nullable=True),
    ]
)


CANADA = "CA"


# The 5 core elements according to readme are:
# PRCP = Precipitation (tenths of mm)
# SNOW = Snowfall (mm)
# SNWD = Snow depth (mm)
# TMAX = Maximum temperature (tenths of degrees C)
# TMIN = Minimum temperature (tenths of degrees C)
# Adding TAVG = Average temperature (tenths of degrees C) to the list of core elements

CORE_ELEMENTS = ["TMAX", "TMIN", "PRCP", "TAVG", "SNOW", "SNWD"]


# The version using csv.gz does not contain header
# compared to the version using csv which contains header
SOURCE_PATH = "data/raw/2024.csv.gz"


def transform_observations(df: DataFrame) -> DataFrame:
    return (
        df.filter(
            (F.substring(F.col("id"), 1, 2) == CANADA)
            & (F.col("element").isin(CORE_ELEMENTS))
        )
        .withColumn("date", F.to_date(F.col("date"), "yyyyMMdd"))
        .withColumn("data_value", F.col("data_value").cast("int"))
        .groupBy("id", "date")
        .pivot("element", CORE_ELEMENTS)
        .agg(F.first("data_value"))
        .toDF("id", "date", "tmax", "tmin", "prcp", "tavg", "snow", "snwd")
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("clean_observations")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(SOURCE_PATH, schema=OBSERVATION_SCHEMA, header=False)
    station_days = transform_observations(df)
    station_days_count = station_days.count()
    print(f"Station days in {SOURCE_PATH}: {station_days_count:,}")

    spark.stop()


if __name__ == "__main__":
    main()
