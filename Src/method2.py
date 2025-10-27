#!/usr/bin/env python3
# Method 2 (Exact): popularity × average dissimilarity to all other tuples (no approximations)
import argparse, math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id, countDistinct, avg, stddev
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def parse_conds(s):
    if s is None or len(s)==0:
        return []
    parts = []
    for tok in s.split(";"):
        tok = tok.strip()
        if not tok:
            continue
        if "=" in tok:
            a,v = tok.split("=",1)
            parts.append((a.strip(), v.strip()))
    return parts

def compute_popularity(spark, R, queries_csv):
    """Compute how many queries each tuple appears in (popularity)."""
    Q = spark.read.option("header","true").csv(queries_csv)
    hits = None
    for row in Q.collect():
        qid = int(row["q_id"])
        conds = parse_conds(row["conds"])
        df = R
        for (a,v) in conds:
            df = df.filter(col(a) == float(v))
        df = df.select("row_id").withColumn("q_id", lit(qid))
        hits = df if hits is None else hits.unionByName(df)
    if hits is None:
        pop = R.select("row_id").withColumn("pop", lit(0))
    else:
        pop = hits.groupBy("row_id").agg(countDistinct("q_id").alias("pop"))
    return R.join(pop, on="row_id", how="left").fillna({"pop":0})

def standardize(df, cols):
    """z-score normalization"""
    stats = df.select(
        *[avg(c).alias(f"{c}_mean") for c in cols],
        *[stddev(c).alias(f"{c}_std") for c in cols]
    ).collect()[0].asDict()
    out = df
    for c in cols:
        mu = float(stats.get(f"{c}_mean", 0.0) or 0.0)
        sd = float(stats.get(f"{c}_std", 1.0) or 1.0)
        if sd == 0: sd = 1.0
        out = out.withColumn(c, (col(c) - lit(mu)) / lit(sd))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, required=True)
    ap.add_argument("--queries", type=str, required=True)
    ap.add_argument("--threshold", type=int, required=True)
    ap.add_argument("--output", type=str, required=True)
    ap.add_argument("--distance", type=str, default="euclidean", choices=["euclidean","cosine"])
    ap.add_argument("--standardize", action="store_true")
    ap.add_argument("--sample_rate", type=float, default=1.0, help="(unused, for main.py compatibility)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("method2_exact_pairwise").getOrCreate()

    # === Step 1. Load dataset ===
    R = spark.read.option("header","true").csv(args.input)
    if "row_id" not in R.columns:
        R = R.withColumn("row_id", monotonically_increasing_id())
    feature_cols = [c for c in R.columns if c != "row_id"]
    for c in feature_cols:
        R = R.withColumn(c, col(c).cast("double"))

    # === Step 2. Popularity ===
    Rp = compute_popularity(spark, R, args.queries).cache()

    # === Step 3. Standardize ===
    X = Rp
    if args.standardize:
        X = standardize(Rp, feature_cols)

    # === Step 4. Exact pairwise dissimilarity ===
    # Convert to pandas (small dataset expected)
    pdf = X.toPandas()
    n = len(pdf)
    if n > 2000:
        print(f"⚠️ Warning: dataset has {n} tuples, pairwise O(n²) computation may be very slow.")

    # Compute pairwise distances
    from numpy import array, dot, sqrt
    import numpy as np

    mat = pdf[feature_cols].to_numpy(dtype=float)
    pop = pdf["pop"].to_numpy(dtype=float)
    n = len(mat)
    imp_vals = np.zeros(n)

    if args.distance == "euclidean":
        for i in range(n):
            diffs = np.sqrt(np.sum((mat[i] - mat)**2, axis=1))
            avg_diff = np.mean(diffs[diffs != 0])
            imp_vals[i] = pop[i] * avg_diff
            if (i+1) % 50 == 0:
                print(f"Computed importance for {i+1}/{n} tuples...")
    else:  # cosine
        norms = np.linalg.norm(mat, axis=1)
        for i in range(n):
            sims = np.zeros(n)
            for j in range(n):
                if norms[i]==0 or norms[j]==0:
                    sims[j] = 0
                else:
                    sims[j] = dot(mat[i], mat[j])/(norms[i]*norms[j])
            diffs = 1 - sims
            avg_diff = np.mean(diffs[diffs != 0])
            imp_vals[i] = pop[i] * avg_diff
            if (i+1) % 50 == 0:
                print(f"Computed importance for {i+1}/{n} tuples...")

    pdf["importance"] = imp_vals

    # === Step 5. Select top T tuples ===
    pdf_sorted = pdf.sort_values(by="importance", ascending=False).head(args.threshold)
    print(f"Selected {len(pdf_sorted)} tuples (threshold={args.threshold})")

    # === Step 6. Save ===
    out_sdf = spark.createDataFrame(pdf_sorted)
    out_sdf.coalesce(1).write.mode("overwrite").option("header","true").csv(args.output)
    print(f"[Exact] Method 2 wrote {len(pdf_sorted)} tuples to {args.output}")

    spark.stop()

if __name__ == "__main__":
    main()
