#!/usr/bin/env python3
import argparse, subprocess, sys, os

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", type=int, required=True, choices=[1,2], help="1 or 2 (method3 omitted in this skeleton)")
    ap.add_argument("--input", type=str, required=True)
    ap.add_argument("--queries", type=str, required=True)
    ap.add_argument("--threshold", type=int, required=True)
    ap.add_argument("--output", type=str, required=True)
    # passthrough args for method2
    ap.add_argument("--distance", type=str, default="euclidean")
    ap.add_argument("--standardize", action="store_true")
    ap.add_argument("--sample_rate", type=float, default=1.0)
    args = ap.parse_args()

    script = None
    if args.method == 1:
        script = os.path.join(os.path.dirname(__file__), "method1.py")
        cmd = [
            sys.executable, script,
            "--input", args.input,
            "--queries", args.queries,
            "--threshold", str(args.threshold),
            "--output", args.output
        ]
    else:
        script = os.path.join(os.path.dirname(__file__), "method2.py")
        cmd = [
            sys.executable, script,
            "--input", args.input,
            "--queries", args.queries,
            "--threshold", str(args.threshold),
            "--output", args.output,
            "--distance", args.distance,
            "--sample_rate", str(args.sample_rate)
        ]
        if args.standardize:
            cmd.append("--standardize")

    # NOTE: When running under spark-submit, this main.py will itself run under Python;
    # you can also call method scripts directly with spark-submit.
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)

if __name__ == "__main__":
    main()
