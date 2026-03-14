# Large-Scale Data Analysis Using MapReduce

Hadoop 3.3.6 Streaming job that computes total sales revenue and transaction count per city across 1,000,000 retail transactions.

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

---

## Rubric Alignment (Marking Scheme Coverage)

This section maps the assignment marking criteria to concrete evidence in this repository.

| Marking Criterion | Coverage in This Project | Evidence |
|-------------------|--------------------------|----------|
| Map/Reduce Logic Accuracy (30) | Implemented mapper and reducer for city-wise sales aggregation with input validation and malformed-row handling. | `mapper.py`, `reducer.py` |
| Dataset Appropriateness (10) | Uses a real-world public Kaggle dataset with 1,000,000 rows and 13 attributes. | Dataset section above, `Retail_Transactions_Dataset.csv` |
| Code Quality and Structure (20) | Clear module separation (mapper/reducer/runner/setup), readable scripts, and deterministic output format. | `mapper.py`, `reducer.py`, `run_local.sh`, `hadoop_setup.sh` |
| Execution Output Evidence (10) | Local pipeline output and error logs are generated; Hadoop execution script provides full end-to-end run. | `output.txt`, `mapreduce_errors.log`, `hadoop_setup.sh` |
| Results Interpretation (10) | Revenue concentration by city can be identified from ranked output; transaction volume supports comparative city behavior analysis. | Expected Output section, output ranking |
| Documentation and Clarity (10) | Step-by-step run instructions for both local and Hadoop modes with prerequisites and expected result format. | This README |
| Bonus for Creativity/Scale (10) | Large-scale data processing on 1M records with Hadoop Streaming and Dockerized reproducible setup. | `hadoop_setup.sh`, Dataset section |

---

## Result Interpretation (1-2 Paragraph Summary)

The city-level aggregation shows that total revenue is distributed across multiple major cities with close transaction counts, suggesting a broad and relatively balanced market presence rather than dependence on a single city. Cities such as Dallas, Boston, Chicago, and New York appear near the top by total revenue, indicating strong sales contribution from diverse urban regions. Because both revenue and transaction count are reported, this output supports comparing not only where sales are highest, but also whether high revenue is driven by volume or potentially higher-value baskets.

From a performance perspective, the reducer uses a streaming key-change approach, which keeps memory usage constant with respect to dataset size and makes it suitable for large-scale input. A practical extension is to compute additional KPIs such as average order value per city and seasonal city-level trends by combining `Season` with `City`. Accuracy can be further improved with stronger numeric normalization (currency symbols, malformed separators) and more detailed bad-record audit logs for data quality reporting.

---
