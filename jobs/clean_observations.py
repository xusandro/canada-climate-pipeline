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


#	   The 5 core elements according to readme are:
# PRCP = Precipitation (tenths of mm)
# SNOW = Snowfall (mm)
# SNWD = Snow depth (mm)
# TMAX = Maximum temperature (tenths of degrees C)
# TMIN = Minimum temperature (tenths of degrees C)
# Adding TAVG = Average temperature (tenths of degrees C) to the list of core elements

CORE_ELEMENTS = ["TMAX", "TMIN", "PRCP", "TAVG", "SNOW", "SNWD"]

path = "data/raw/2024.csv.gz"



def transform_observations(df: DataFrame) -> DataFrame:
    return df.filter(F.substring(F.col('id'), 1, 2) == CANADA)



def main() -> None:
    spark = (
        SparkSession.builder.appName("clean_observations")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(path, schema=OBSERVATION_SCHEMA, header=False)
    canadian_observations = transform_observations(df)
    canadian_observations_count = canadian_observations.count()
    print(f"Canadian observations in {path}: {canadian_observations_count:,}")

    spark.stop()

if __name__ == "__main__":
    main()