# Infrastructure

The AWS resources this pipeline runs on. Created through the console; the policy
documents are kept here so the permissions granted are visible and reproducible rather
than only existing as console state.

## S3

One bucket, `s3://canada-climate-pipeline` (`us-east-1`, all public access blocked).

`us-east-1` is not arbitrary: the NOAA source bucket lives there, so keeping the job in
the same region avoids cross-region transfer entirely (see ADR-002).

```
s3://canada-climate-pipeline/
  scripts/                          Glue job script
  temp/                             Glue scratch space
  processed/observations.parquet/   cleaned output,   year=/month=
  quarantined/observations.parquet/ rejected records, year=/month=
```

## IAM role — `GlueCanadaClimatePipeline`

**Trust policy** (`glue-trust-policy.json`) — only the Glue service may assume the role.

**Attached managed policy** — `AWSGlueServiceRole`, which grants the Glue service basics
and CloudWatch Logs.

**Inline policy** (`glue-s3-policy.json`), two statements:

| Statement | Grants | Why |
|---|---|---|
| `ReadNoaaOpenDataSource` | `GetObject`, `ListBucket` on `noaa-ghcn-pds` | The bucket is public, but IAM requires **both** the resource policy and the caller's identity policy to allow the action. A public bucket does not exempt the role from needing its own permission. |
| `ReadWriteProjectBucket` | `GetObject`, `PutObject`, `DeleteObject`, `ListBucket` on the project bucket | `DeleteObject` is **required**, not incidental: dynamic partition overwrite replaces a partition by deleting the old files first. Without it the job fails only on a re-run, not on the first write. |

Scoped to these two buckets by ARN rather than `"Resource": "*"`.

## IAM role — `RedshiftCanadaClimatePipeline`

**Trust policy** (`redshift-trust-policy.json`) — only the Redshift service may assume the role.

**Inline policy** (`redshift-s3-policy.json`), one statement:

| Statement | Grants | Why |
|---|---|---|
| `ReadProjectBucketForCopy` | `GetObject`, `ListBucket` on the project bucket | `COPY` is run by Redshift itself, not by the client: the service assumes this role to read from S3, so no credentials ever appear in the SQL. Read-only — `UNLOAD`, which writes back to S3, is not used. |

Deliberately narrower than the console default, `AmazonRedshiftAllCommandsFullAccess`, which
also grants SageMaker, Glue and CloudWatch access. Redshift Spectrum is **not** covered; it
would need Glue Data Catalog permissions, to be added only if the warehouse ever queries the
lake directly.

Created from the CLI rather than the console, because the console sign-in identity lacks
`iam:CreateRole`.
