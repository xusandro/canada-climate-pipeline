from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import argparse
import sys

LOCAL = "--JOB_NAME" not in sys.argv


STATIONS_LAYOUT = [
    ("id", 1, 11),
    ("latitude", 13, 20),
    ("longitude", 22, 30),
    ("elevation", 32, 37),
    ("state", 39, 40),
    ("name", 42, 71),
    ("gsn_flag", 73, 75),
    ("hcn_crn_flag", 77, 79),
    ("wmo_id", 81, 85),
]


def read_fixed_width(
    spark: SparkSession, path:str, layout:list[tuple[str, int, int]]
) -> DataFrame :
    
    raw = spark.read.text(path)

    columns = []

    for name, start, end in layout:
        length = end - start + 1

        column = F.substring(F.col("value"), start, length)
        column = F.nullif(F.trim(column), F.lit("")).alias(name)
        columns.append(column)

    # The * here indicates that the list of columns is unpacked into individual arguments for the select function
    return raw.select(*columns)


CANADA = "CA"

def transform_stations(df:DataFrame) -> DataFrame:
    return(
        df.filter(F.substring(F.col("id"), 1, 2) == CANADA)
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("elevation", F.col("elevation").cast("double"))
    )

SOURCE_PATH = "data/raw/ghcnd-stations.txt"
OUTPUT_PATH = "data/processed/stations.parquet"

def write_stations(df:DataFrame, out_path:str) -> None:
    df.write.mode("overwrite").parquet(out_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_path",
        type = str,
        default = SOURCE_PATH
    )

    parser.add_argument(
        "--output_path",
        type = str,
        default = OUTPUT_PATH
    )
    return parser



def build_spark(local: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("stations_dim")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if local:
        builder = builder.master("local[*]").config("spark.driver.memory", "4g")

    return builder.getOrCreate()

def main() -> None:
    spark = build_spark(LOCAL)
    spark.sparkContext.setLogLevel("ERROR")
    args, unknown = build_parser().parse_known_args()

    try:
        stations = read_fixed_width(
            spark,
            args.source_path,
            STATIONS_LAYOUT,
        )

        canadian_stations = transform_stations(stations).cache()

        canadian_stations.show(10, truncate=False)

        print(
            f"Canadian stations: "
            f"{canadian_stations.count():,}"
        )

        (
            canadian_stations
            .groupBy("state")
            .count()
            .orderBy(F.desc("count"))
            .show(20)
        )

        print(
            f"Canadian stations with wmo null: ",
            canadian_stations
            .filter(F.col("wmo_id").isNull())
            .count()
        )

        write_stations(canadian_stations, args.output_path)
        canadian_stations.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()