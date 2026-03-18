import argparse
import yaml
import json
import logging
import time
import os
import sys
import pandas as pd
import numpy as np

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

def write_metrics(output_file, data):
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)
    # The requirement asks to Print final metrics JSON to stdout
    print(json.dumps(data, indent=4))

def main():
    parser = argparse.ArgumentParser(description="MLOps Batch Job")
    parser.add_argument('--input', required=True, help='Path to input CSV data')
    parser.add_argument('--config', required=True, help='Path to configuration YAML')
    parser.add_argument('--output', required=True, help='Path to output metrics JSON')
    parser.add_argument('--log-file', required=True, help='Path to run log file')
    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("Job started.")

    metrics_output = {
        "version": "unknown",
        "status": "error",
        "error_message": "Unknown error"
    }
    
    has_error = False

    try:
        # 1. Load + validate config
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found: {args.config}")
            
        with open(args.config, 'r') as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(f"Invalid config structure: {e}")

        if not isinstance(config, dict):
            raise ValueError("Invalid config structure: must be a dictionary")
            
        required_keys = ['seed', 'window', 'version']
        missing_keys = [k for k in required_keys if k not in config]
        if missing_keys:
            raise ValueError(f"Config missing required fields: {', '.join(missing_keys)}")

        seed = config['seed']
        window = config['window']
        version = config['version']
        
        metrics_output["version"] = version
        
        np.random.seed(seed)
        logging.info(f"Config loaded and validated. version: {version}, seed: {seed}, window: {window}")

        # 2. Load + validate dataset
        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Missing input file: {args.input}")

        try:
            df = pd.read_csv(args.input)
        except pd.errors.EmptyDataError:
            raise ValueError("Empty file")
        except Exception as e:
            raise ValueError(f"Invalid CSV format: {e}")

        if df.empty:
            raise ValueError("Empty file")

        if 'close' not in df.columns:
            raise ValueError("Missing required column (close)")

        rows_loaded = len(df)
        logging.info(f"Loaded {rows_loaded} rows from {args.input}")

        # 3. Rolling mean
        start_time = time.time()
        logging.info(f"Computing rolling mean with window {window}")
        df['rolling_mean'] = df['close'].rolling(window=window).mean()

        # Handle NaN explicitly
        df = df.dropna(subset=['rolling_mean'])
        logging.info("First window-1 rows excluded due to NaN rolling mean")

        # 4. Signal
        logging.info("Generating signals")
        # For each row: signal = 1 if close > rolling_mean else 0
        df['signal'] = np.where(df['close'] > df['rolling_mean'], 1, 0)
        
        # 5. Metrics + timing
        rows_processed = len(df)
        signal_rate = float(df['signal'].mean())
        latency_ms = int((time.time() - start_time) * 1000)

        metrics_output = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }
        
        logging.info("Processing complete.")
        logging.info(f"Metrics summary: rows_processed={rows_processed}, signal_rate={signal_rate:.4f}, latency_ms={latency_ms}")
        logging.info("Job end - Status: SUCCESS")

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Job failed with error: {error_msg}")
        metrics_output["status"] = "error"
        metrics_output["error_message"] = error_msg
        logging.info("Job end - Status: ERROR")
        has_error = True
        
    write_metrics(args.output, metrics_output)
    if has_error:
        sys.exit(1)

if __name__ == "__main__":
    main()
