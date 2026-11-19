# Troubleshooting log

Real problems met while building this pipeline, and how they were solved. Useful if you hit the same errors.

## Data / pandas

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ParserError: Expected 2 fields in line 111, saw ...` | Polish Excel saves CSV with `;` as separator | `pd.read_csv(..., sep=";")` |
| `time data "13.12.2010 09:02" doesn't match format "%m.%d.%Y"` | pandas guessed US date order | `pd.to_datetime(..., format="%d.%m.%Y %H:%M")` |
| `ValueError: Unable to parse string '2,55'` | Decimal comma in `UnitPrice` | `pd.read_csv(..., decimal=",")` (plus a defensive `.str.replace(",", ".")`) |
| `UnicodeDecodeError` with `utf-8` | Windows-1252 style export | `encoding="latin1"` |
| `KeyError: ['Total_Amount'] not in index` | Silver table column is named `total_amount` (lower case) | Fact table now reads `total_amount` explicitly |
| `NULL` `ProductKey` in the fact table | Same `StockCode` has several descriptions; joining on `StockCode + Description` loses matches | One dimension row per `StockCode`, join on `StockCode` only |

## Airflow / Docker

| Symptom | Cause | Fix |
|---------|-------|-----|
| DAG not visible | `dags/` folder missing or file placed next to `docker-compose.yaml` | File must be in `dags/` (mounted to `/opt/airflow/dags`) |
| `TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'` | Removed in Airflow 3 | Use `schedule=None` |
| `IndentationError` in DAG | Mixed indentation after copy/paste | Consistent 4 spaces |
| `ModuleNotFoundError: openpyxl` / `Permission denied` on `pip install` | Containers run as a non-root user; ad-hoc `pip install` is lost on recreate | Use `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`, or switch to CSV (no extra package needed) |
| `FileNotFoundError: /opt/airflow/dags/Online Retail.csv` | File missing or saved with a different extension | Put `Online Retail.csv` in `dags/`; the DAG builds the path relative to its own location |
| New variables from `.env` not visible in the container | `docker compose restart` does not re-read `env_file` | `docker compose down && docker compose up -d` |
| Cannot reach PostgreSQL from a container | `localhost` inside a container is the container itself | Use `host.docker.internal` |

## AWS

| Symptom | Cause | Fix |
|---------|-------|-----|
| `NoSuchBucket` | Bucket not created (names are globally unique) | Create the bucket in the same region as `S3_REGION_NAME` |
| `AccessDenied ... s3:PutObject` | IAM user has no S3 permissions | Attach [iam-policy.json](iam-policy.json) (least privilege) |
| `NoCredentialsError: Unable to locate credentials` | AWS keys not available in the container | Put `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in `.env`, recreate containers |
| Region error with `Stockholm` | boto3 needs region codes, not names | Use `eu-north-1` |

## Security notes

- Never put access keys in code, notebooks, screenshots or chat logs. If a key leaks, deactivate and delete it in IAM and create a new one.
- Do not commit `.env`, `~/.aws/credentials` or exported chat transcripts containing keys.
