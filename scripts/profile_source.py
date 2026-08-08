"""Profile the raw GHCN-Daily source data.

Exploratory, not part of the pipeline. Its purpose is to replace the assumptions in
the open decisions of DECISIONS.md (processing scope, year range, station selection)
with measured numbers.

Run against the locally downloaded sample:

    python scripts/profile_source.py --year-file data/raw/2024.csv.gz \
        --inventory data/raw/ghcnd-inventory.txt \
        --stations data/raw/ghcnd-stations.txt
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# The by_year CSVs carry no header row. Every column is read as a string so that
# malformed values surface as data rather than as silent nulls from a failed cast.
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

# Fixed-width layouts, from the field definitions in the dataset's readme.txt.
# (name, start, end) with 1-based inclusive columns, as the readme states them.
INVENTORY_LAYOUT = [
    ("id", 1, 11),
    ("latitude", 13, 20),
    ("longitude", 22, 30),
    ("element", 32, 35),
    ("first_year", 37, 40),
    ("last_year", 42, 45),
]

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

# GHCN station ids begin with a two-letter FIPS country code.
CANADA = "CA"

# The elements this pipeline ultimately models. Everything else is a long tail.
CORE_ELEMENTS = ["TMAX", "TMIN", "PRCP", "TAVG", "SNOW", "SNWD"]

MISSING_SENTINEL = "-9999"


def read_fixed_width(
    spark: SparkSession, path: str, layout: list[tuple[str, int, int]]
) -> DataFrame:
    """Read a fixed-width text file into typed-as-string columns, trimming each field."""
    raw = spark.read.text(path)
    return raw.select(
        *[
            F.trim(F.substring(F.col("value"), start, end - start + 1)).alias(name)
            for name, start, end in layout
        ]
    )


# Prints a section header for better view
def section(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def profile_observations(observations: DataFrame) -> None:
    """Volume, country mix, element mix and quality-flag mix for one year of data."""
    country = F.substring(F.col("id"), 1, 2).alias("country")
    enriched = observations.withColumn("country", country).cache()

    section("1. Volume")
    total_rows = enriched.count()
    total_stations = enriched.select("id").distinct().count()
    print(f"rows            {total_rows:>14,}")
    print(f"distinct stations {total_stations:>12,}")

    section("2. Country mix — how much of the file is Canadian (OD-1)")
    by_country = (
        enriched.groupBy("country")
        .agg(F.count("*").alias("rows"), F.countDistinct("id").alias("stations"))
        .withColumn("pct_rows", F.round(100 * F.col("rows") / F.lit(total_rows), 2))
        .orderBy(F.desc("rows"))
    )
    by_country.show(10, truncate=False)

    canada = by_country.where(F.col("country") == CANADA).collect()
    if canada:
        row = canada[0]
        print(
            f"Canada: {row['rows']:,} rows ({row['pct_rows']}%) across {row['stations']:,} stations"
        )
        print(f"Ratio full:Canada = {total_rows / row['rows']:.1f}x")

    section("3. Element mix — which measurements actually exist")
    (
        enriched.groupBy("element")
        .agg(F.count("*").alias("rows"))
        .withColumn("pct", F.round(100 * F.col("rows") / F.lit(total_rows), 2))
        .orderBy(F.desc("rows"))
        .show(15, truncate=False)
    )

    print("Core elements only, Canada only:")
    (
        enriched.where(
            (F.col("country") == CANADA) & F.col("element").isin(CORE_ELEMENTS)
        )
        .groupBy("element")
        .agg(F.count("*").alias("rows"), F.countDistinct("id").alias("stations"))
        .orderBy(F.desc("rows"))
        .show(truncate=False)
    )

    section("4. Quality flags — how much would the quarantine path catch (ADR-006)")
    flagged = (
        enriched.groupBy("q_flag")
        .agg(F.count("*").alias("rows"))
        .withColumn("pct", F.round(100 * F.col("rows") / F.lit(total_rows), 4))
        .orderBy(F.desc("rows"))
    )
    flagged.show(20, truncate=False)

    failed_qc = enriched.where(
        F.col("q_flag").isNotNull() & (F.col("q_flag") != "")
    ).count()
    print(f"rows failing QC {failed_qc:>14,}  ({100 * failed_qc / total_rows:.4f}%)")

    section("5. Missing-value sentinel and date range")
    sentinel = enriched.where(F.col("data_value") == MISSING_SENTINEL).count()
    print(f"-9999 rows      {sentinel:>14,}")
    non_numeric = enriched.where(F.col("data_value").cast("int").isNull()).count()
    print(f"non-numeric data_value {non_numeric:>9,}")
    enriched.select(
        F.min("date").alias("min_date"), F.max("date").alias("max_date")
    ).show()

    print("obs_time populated:")
    (
        enriched.select(
            F.count("*").alias("total"),
            F.sum(
                F.when(
                    F.col("obs_time").isNotNull() & (F.col("obs_time") != ""), 1
                ).otherwise(0)
            ).alias("with_obs_time"),
        ).show()
    )

    enriched.unpersist()


def profile_inventory(inventory: DataFrame) -> None:
    """Station coverage over time — the evidence for the year range (OD-2) and the
    valid station set (OD-3)."""
    canadian = (
        inventory.where(F.substring(F.col("id"), 1, 2) == CANADA)
        .withColumn("first_year", F.col("first_year").cast("int"))
        .withColumn("last_year", F.col("last_year").cast("int"))
        .cache()
    )

    section("6. Canadian station inventory")
    print(f"Canadian station-element pairs {canadian.count():,}")
    print(
        f"Distinct Canadian stations     {canadian.select('id').distinct().count():,}"
    )

    (
        canadian.where(F.col("element").isin(CORE_ELEMENTS))
        .groupBy("element")
        .agg(
            F.countDistinct("id").alias("stations"),
            F.min("first_year").alias("earliest"),
            F.max("last_year").alias("latest"),
        )
        .orderBy(F.desc("stations"))
        .show(truncate=False)
    )

    section(
        "7. Coverage windows for TMAX — how many stations span a window (OD-2/OD-3)"
    )
    # The inventory gives only the two endpoints of a station's record, so a station
    # listed as 1950-2026 may still have gaps in between. These counts therefore measure
    # "the record SPANS the window", which is an UPPER BOUND on the number of stations
    # with an unbroken record. True continuity can only be measured against the
    # observations themselves, once every year in the window has been ingested.
    tmax = canadian.where(F.col("element") == "TMAX")
    for start in (1990, 2000, 2010):
        spanning = tmax.where(
            (F.col("first_year") <= start) & (F.col("last_year") >= 2025)
        ).count()
        print(
            f"stations whose TMAX record spans {start}-2025: {spanning:>6,}  (upper bound)"
        )

    print(
        "\nStations reporting TMAX in a given year (station count by decade of first record):"
    )
    (
        tmax.withColumn("first_decade", (F.col("first_year") / 10).cast("int") * 10)
        .groupBy("first_decade")
        .agg(F.count("*").alias("stations"))
        .orderBy("first_decade")
        .show(20, truncate=False)
    )

    print("Last year of record — how many stations are still active:")
    (
        tmax.groupBy("last_year")
        .agg(F.count("*").alias("stations"))
        .orderBy(F.desc("last_year"))
        .show(10, truncate=False)
    )

    canadian.unpersist()


def profile_stations(stations: DataFrame) -> None:
    section("8. Station metadata (dimension source)")
    canadian = stations.where(F.substring(F.col("id"), 1, 2) == CANADA)
    print(f"Canadian stations in ghcnd-stations.txt: {canadian.count():,}")
    canadian.select("id", "latitude", "longitude", "elevation", "state", "name").show(
        10, truncate=False
    )

    print("Stations per province/territory code:")
    (
        canadian.groupBy("state")
        .agg(F.count("*").alias("stations"))
        .orderBy(F.desc("stations"))
        .show(20, truncate=False)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--year-file", required=True, help="path to a by_year CSV, e.g. 2024.csv.gz"
    )
    parser.add_argument(
        "--inventory", required=True, help="path to ghcnd-inventory.txt"
    )
    parser.add_argument("--stations", required=True, help="path to ghcnd-stations.txt")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    for path in (args.year_file, args.inventory, args.stations):
        if not Path(path).exists():
            raise SystemExit(f"missing input: {path}")

    spark = (
        SparkSession.builder.appName("ghcn-source-profile")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        observations = spark.read.csv(
            args.year_file, schema=OBSERVATION_SCHEMA, header=False
        )
        profile_observations(observations)
        profile_inventory(read_fixed_width(spark, args.inventory, INVENTORY_LAYOUT))
        profile_stations(read_fixed_width(spark, args.stations, STATIONS_LAYOUT))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
