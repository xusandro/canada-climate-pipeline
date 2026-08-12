#!/usr/bin/env bash
# Backfill the processed zone one year at a time.
#
# One Glue job run per year, which keeps the reprocessing unit equal to the
# scheduling unit (ADR-003): a year that fails is retried on its own, and every run is
# idempotent because dynamic partition overwrite replaces only that year's partitions.
#
#   ./scripts/backfill.sh              # 1990-2025, six at a time
#   ./scripts/backfill.sh 2000 2010    # a narrower range

set -euo pipefail

JOB_NAME="canada-climate-clean-observations"
SOURCE_PREFIX="s3://noaa-ghcn-pds/csv.gz/by_year"
OUTPUT_PREFIX="s3://canada-climate-pipeline"
MAX_PARALLEL=6          # must be <= the job's Maximum concurrency setting
POLL_SECONDS=20

START_YEAR="${1:-1990}"
END_YEAR="${2:-2025}"

# A run occupies a concurrency slot from the moment it is submitted, and it spends the
# first minute or so in STARTING while Glue provisions the cluster. Counting only
# RUNNING undercounts, submits too many, and trips ConcurrentRunsExceededException.
ACTIVE_STATES="'STARTING','RUNNING','STOPPING','WAITING'"

running_count() {
  aws glue get-job-runs --job-name "$JOB_NAME" \
    --query "length(JobRuns[?contains([$ACTIVE_STATES], JobRunState)])" --output text
}

echo "Backfilling $START_YEAR-$END_YEAR, up to $MAX_PARALLEL runs in flight"

for year in $(seq "$START_YEAR" "$END_YEAR"); do
  while [ "$(running_count)" -ge "$MAX_PARALLEL" ]; do
    sleep "$POLL_SECONDS"
  done

  # The argument names Glue expects start with "--", which the CLI's shorthand
  # key=value syntax parses as new options. JSON is the way to pass them.
  # Submission still races against other runs changing state, so retry rather than abort.
  until run_id=$(aws glue start-job-run \
      --job-name "$JOB_NAME" \
      --arguments "{\"--year\":\"$year\",\"--source_prefix\":\"$SOURCE_PREFIX\",\"--output_prefix\":\"$OUTPUT_PREFIX\"}" \
      --query JobRunId --output text 2>/dev/null); do
    sleep "$POLL_SECONDS"
  done

  echo "  $year  started  $run_id"
done

echo "All runs submitted. Waiting for the last ones to finish..."
while [ "$(running_count)" -gt 0 ]; do
  sleep "$POLL_SECONDS"
done

echo
echo "Run states:"
aws glue get-job-runs --job-name "$JOB_NAME" \
  --query "JobRuns[?starts_with(to_string(Arguments.\"--year\"), '2') || starts_with(to_string(Arguments.\"--year\"), '1')].[Arguments.\"--year\",JobRunState,ExecutionTime]" \
  --output table
