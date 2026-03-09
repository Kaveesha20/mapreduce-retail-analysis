# Large-Scale Data Analysis Using MapReduce

Hadoop 3.3.6 Streaming job that computes total sales revenue and transaction count per city across 1,000,000 retail transactions.

---

## Team Members

| Name | Registration Number |
|------|---------------------|
|      |                     |
|      |                     |
|      |                     |

---

## Dataset

| Field | Details |
|-------|---------|
| Name | Retail Transactions Dataset |
| Source | https://www.kaggle.com/datasets/prasad22/retail-transactions-dataset |
| Rows | 1,000,000 |
| Columns used | `City` (index 7), `Total_Cost` (index 5) |
| All columns | Transaction_ID, Date, Customer_Name, Product, Total_Items, Total_Cost, Payment_Method, City, Store_Type, Discount_Applied, Customer_Category, Season, Promotion |

---

## MapReduce Task

The job computes **total sales revenue and transaction count per city** from 1 million retail transactions. The mapper reads each CSV row via `csv.reader`, skips the header, extracts the `City` and `Total_Cost` fields (validating both and logging any skipped rows to stderr), then emits a tab-separated `city\ttotal_cost` pair. Hadoop's shuffle and sort phase groups all pairs by city key. The reducer uses a key-change flush pattern — O(1) memory — to accumulate revenue and count transactions for each city, outputting `city\ttotal_revenue\ttransaction_count` once the key changes.

---

## Prerequisites

The following were used:

- Docker Desktop 27.4.0
- Python 3.11.9
- Git Bash (Windows)
- Apache Hadoop 3.3.6 (via `apache/hadoop:3` Docker image)

---

## How to Run

### Method A — Local Test (no Hadoop needed)

Simulates the MapReduce pipeline using Unix pipes (`mapper | sort | reducer`). Runs entirely on the host machine — no Docker required.

```bash
bash run_local.sh
```

Output is saved to `output.txt`. Skipped rows (if any) are logged to `mapreduce_errors.log`.

---

### Method B — Full Hadoop on Docker

Starts a real Hadoop 3.3.6 YARN cluster in Docker, uploads the dataset to HDFS, and runs the Streaming job.

```bash
bash hadoop_setup.sh
```

The script automates all 8 steps: pull image → start container → install Python 3 → configure and start HDFS/YARN → upload dataset → run job → retrieve output.

After the job completes:
- HDFS NameNode UI → http://localhost:9870
- YARN ResourceManager UI → http://localhost:8088

To stop and clean up:
```bash
docker stop hadoop-mr
docker rm -f hadoop-mr
```

---

## Expected Output

Top 5 cities by revenue (full results in `output.txt`):

| City | Total Revenue (USD) | Transactions |
|------|---------------------|--------------|
| Dallas | 5,277,111.53 | 100,559 |
| Boston | 5,263,307.96 | 100,566 |
| Chicago | 5,263,187.45 | 100,059 |
| New York | 5,252,469.92 | 100,007 |
| Houston | 5,247,054.78 | 100,050 |

---

## Project Structure

| File | Description |
|------|-------------|
| `mapper.py` | Hadoop Streaming mapper — reads CSV rows, emits `city\ttotal_cost` |
| `reducer.py` | Hadoop Streaming reducer — aggregates revenue and count per city |
| `run_local.sh` | Local pipeline runner using Unix pipes (no Hadoop required) |
| `hadoop_setup.sh` | Full Hadoop Docker setup and job execution (8 automated steps) |
| `output.txt` | Final output — one tab-separated row per city |
| `mapreduce_errors.log` | Skipped/malformed rows logged during local run |
| `Retail_Transactions_Dataset.csv` | Input dataset — 1,000,000 rows, 13 columns |
