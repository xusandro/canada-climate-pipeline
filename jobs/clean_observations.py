from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
import argparse

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
DEFAULT_SOURCE_PREFIX = "data/raw"
DEFAULT_OUTPUT_PREFIX = "data"


# Only does preparation of data which can be cached for use
def prepare_observations(df:DataFrame) -> DataFrame:
    selected_data = ( df.filter(
                    (F.substring(F.col("id"), 1, 2) == CANADA)
                    & (F.col("element").isin(CORE_ELEMENTS))
                )
                .withColumn("date", F.to_date(F.col("date"), "yyyyMMdd"))
                .withColumn("data_value", F.col("data_value").cast("int"))
                .withColumn("year", F.year(F.col("date")))
                .withColumn("month", F.date_format(F.col("date"), "MM"))
        )
    return selected_data



def split_by_quality(selected_data: DataFrame) -> tuple[DataFrame, DataFrame]:

    clean = selected_data.filter(F.col("q_flag").isNull())
    rejected = selected_data.filter(F.col("q_flag").isNotNull())
    return clean, rejected


def transform_observations(clean_data: DataFrame) -> DataFrame:
    return (
        clean_data.groupBy("id", "date")
        .pivot("element", CORE_ELEMENTS)
        .agg(F.first("data_value"))
        .toDF("id", "date", "tmax", "tmin", "prcp", "tavg", "snow", "snwd")
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.date_format(F.col("date"), "MM"))
        .withColumn("tmax", F.col("tmax") / 10)
        .withColumn("tmin", F.col("tmin") / 10)
        .withColumn("tavg", F.col("tavg") / 10)
        .withColumn("prcp", F.col("prcp") / 10)
    )

def write_clean_data(df: DataFrame, path: str) -> None:
    (df.repartition("year", "month")
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(f"{path}/processed/observations.parquet")
     )

def write_quarantined_data(df: DataFrame, path: str) -> None:
    (df.repartition("year", "month")
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(f"{path}/quarantined/observations.parquet")
     )

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year of the observations to process (e.g., 2024)"
    )
    parser.add_argument(
        "--source_prefix",
        type=str,
        default=DEFAULT_SOURCE_PREFIX,
        help="Path to the source CSV file (default: data/raw)",
    )
    parser.add_argument(
        "--output_prefix",
        type=str,
        default=DEFAULT_OUTPUT_PREFIX,
        help="Path to the source CSV file (default: data)",
    )
    return parser




def main() -> None:

    args = build_parser().parse_args()
        
    spark = (
        SparkSession.builder.appName("clean_observations")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )

    spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
    )

    spark.sparkContext.setLogLevel("ERROR")
    source = f"{args.source_prefix}/{args.year}.csv.gz"
    df = spark.read.csv(source, schema=OBSERVATION_SCHEMA, header=False)
    prepared = prepare_observations(df).cache()
    clean, rejected = split_by_quality(prepared)
    write_quarantined_data(rejected, args.output_prefix)
    station_days = transform_observations(clean).cache()
    write_clean_data(station_days, args.output_prefix)
    station_days_count = station_days.count()
    print(f"Station days in {source}: {station_days_count:,}")
    print(f"Rejected rows in {source} according to Q_flag: {rejected.count():,}")
    prepared.unpersist()
    station_days.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
