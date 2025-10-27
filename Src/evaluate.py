#!/usr/bin/env python3
# Lightweight evaluation harness: generate data/queries and run methods for a few scales.
import argparse, os, subprocess, sys

PY = sys.executable

def run(cmd):
    print(">>", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base_rows", type=int, default=20000)
    ap.add_argument("--scales", type=str, default="1,2,4")
    ap.add_argument("--cols", type=int, default=6)
    ap.add_argument("--num_queries", type=int, default=500)
    ap.add_argument("--threshold", type=int, default=500)
    args = ap.parse_args()

    scales = [int(s) for s in args.scales.split(",")]
    for s in scales:
        rows = args.base_rows * s
        r_csv = f"Data/synth_R_{rows}.csv"
        q_csv = f"Data/synth_Q_{rows}.csv"
        out1 = f"Data/results/m1_{rows}.csv"
        out2 = f"Data/results/m2_{rows}.csv"

        run(["spark-submit","Src/generate_dataset.py","--rows",str(rows),"--cols",str(args.cols),"--out",r_csv])
        run(["spark-submit","Src/generate_queries.py","--num_queries",str(args.num_queries),"--cols",str(args.cols),"--in",r_csv,"--out",q_csv])

        run(["spark-submit","Src/main.py","--method","1","--input",r_csv,"--queries",q_csv,"--threshold",str(args.threshold),"--output",out1])
        run(["spark-submit","Src/main.py","--method","2","--input",r_csv,"--queries",q_csv,"--threshold",str(args.threshold),"--output",out2,"--standardize","--distance","euclidean"])

    print("Done. Inspect outputs in Data/results/.")

if __name__ == "__main__":
    main()
