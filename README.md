# MLOps Batch Signal Pipeline
## Overview

This project implements a minimal batch pipeline that processes OHLCV data to generate trading signals.

It demonstrates:

* Deterministic execution via configuration and seed
* Structured logging for observability
* Machine-readable metrics output
* Dockerized execution for reproducibility

---

## Design Decisions

* The rolling mean is computed on the `close` column using a configurable window.
* The first (window - 1) rows are excluded since the rolling mean is undefined.
* Signal is defined as:

  * 1 if close > rolling mean
  * 0 otherwise
* Latency measures only the processing time (excluding file I/O and argument parsing).
* A random seed is set for reproducibility, even though the pipeline itself is deterministic.

---

## Local Run Instructions

Ensure Python 3.9+ is installed.

```bash
# Optional: create virtual environment
# python -m venv venv
# source venv/bin/activate
# pip install -r requirements.txt

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## Docker Build & Run

```bash
docker build -t mlops-task .
docker run --rm mlops-task
```

---

## Example Output (`metrics.json`)

```json
{
    "version": "v1",
    "rows_processed": 9996,
    "metric": "signal rate",
    "value": 0.4974,
    "latency_ms": 4,
    "seed": 42,
    "status": "success"
}
```
