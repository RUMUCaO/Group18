#!/usr/bin/env python3
import argparse, csv, random
from pyspark.sql import SparkSession

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--num_queries", type=int, default=500)
    ap.add_argument("--cols", type=int, default=6)
    ap.add_argument("--tightness", type=int, default=2, help="number of equality clauses per query")
    ap.add_argument("--in", dest="input_csv", type=str, default="Data/synthetic_R.csv")
    ap.add_argument("--out", type=str, default="Data/synthetic_Q.csv")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("generate_queries").getOrCreate()
    df = spark.read.option("header", "true").csv(args.input_csv)
    all_cols = [c for c in df.columns if c != "row_id"][:args.cols]

    # For each column, sample some values to draw from
    value_samples = {}
    for c in all_cols:
        vals = [r[c] for r in df.select(c).dropna().limit(5000).collect()]
        if not vals:
            vals = ["0"]
        value_samples[c] = vals

    queries = []
    for q_id in range(args.num_queries):
        chosen_cols = random.sample(all_cols, k=min(args.tightness, len(all_cols)))
        clauses = []
        for c in chosen_cols:
            v = random.choice(value_samples[c])
            clauses.append(f"{c}={v}")
        queries.append((q_id, ";".join(clauses)))

    # write CSV
    out_path = args.out
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["q_id", "conds"])
        for q_id, conds in queries:
            w.writerow([q_id, conds])

    print(f"Wrote queries to {out_path}")
    spark.stop()

if __name__ == "__main__":
    main()
