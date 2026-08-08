"""Compare Canadian station coverage across sample years.

Answers a question the single-year profile cannot: does the set of reporting stations
change over time? If it does, a year-over-year temperature average is measuring station
composition as much as it is measuring climate.

    python scripts/profile_coverage.py data/raw/1990.csv.gz data/raw/2005.csv.gz data/raw/2024.csv.gz
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from profile_source import CANADA, CORE_ELEMENTS, OBSERVATION_SCHEMA

YEAR_IN_FILENAME = re.compile(r"(\d{4})")


def year_of(path: str) -> str:
    match = YEAR_IN_FILENAME.search(Path(path).name)
    if not match:
        raise SystemExit(f"cannot infer year from filename: {path}")
    return match.group(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("year_files", nargs="+", help="by_year CSV files, e.g. data/raw/1990.csv.gz")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("ghcn-coverage-profile")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    rows = []
    try:
        for path in args.year_files:
            year = year_of(path)
            df = spark.read.csv(path, schema=OBSERVATION_SCHEMA, header=False)
            canadian = df.where(F.substring(F.col("id"), 1, 2) == CANADA).cache()

            per_element = {
                r["element"]: (r["rows"], r["stations"])
                for r in canadian.where(F.col("element").isin(CORE_ELEMENTS))
                .groupBy("element")
                .agg(F.count("*").alias("rows"), F.countDistinct("id").alias("stations"))
                .collect()
            }
            rows.append(
                {
                    "year": year,
                    "total_rows": df.count(),
                    "ca_rows": canadian.count(),
                    "ca_stations": canadian.select("id").distinct().count(),
                    "tmax_stations": per_element.get("TMAX", (0, 0))[1],
                    "prcp_stations": per_element.get("PRCP", (0, 0))[1],
                    "tavg_stations": per_element.get("TAVG", (0, 0))[1],
                }
            )
            canadian.unpersist()

        header = f"{'year':<6}{'total rows':>14}{'CA rows':>12}{'CA stns':>9}{'TMAX':>7}{'PRCP':>7}{'TAVG':>7}{'CA %':>7}"
        print(f"\n{header}\n{'-' * len(header)}")
        for r in rows:
            pct = 100 * r["ca_rows"] / r["total_rows"]
            print(
                f"{r['year']:<6}{r['total_rows']:>14,}{r['ca_rows']:>12,}{r['ca_stations']:>9,}"
                f"{r['tmax_stations']:>7,}{r['prcp_stations']:>7,}{r['tavg_stations']:>7,}{pct:>6.2f}%"
            )
        print()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
