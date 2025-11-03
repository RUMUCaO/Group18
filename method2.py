#!/usr/bin/env python3
# Method 2 (Exact): popularity × average dissimilarity to all other tuples
import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\rumucao\AppData\Local\Microsoft\WindowsApps\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\rumucao\AppData\Local\Microsoft\WindowsApps\python.exe'

import os
# 临时绕过 Hadoop 检查
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + os.path.join("C:\\hadoop", "bin")

import argparse, csv, time, numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id, countDistinct, avg, stddev

# =============================
# Utility: parse query conditions
# =============================

def get_spark():
    spark = (
        SparkSession.builder
        .appName("method2_exact_pairwise")
        .config("spark.driver.memory", "22g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
    return spark
    
def parse_conds(s):
    if s is None or len(s) == 0:
        return []
    parts = []
    for tok in s.split(";"):
        tok = tok.strip()
        if not tok:
            continue
        if "=" in tok:
            a, v = tok.split("=", 1)
            parts.append((a.strip(), v.strip()))
    return parts

# =============================
# Compute popularity for each tuple
# =============================
def compute_popularity(spark, R, queries_csv):
    Q = spark.read.option("header", "true").csv(queries_csv)
    hits = None
    for row in Q.collect():
        qid = int(row["q_id"])
        conds = parse_conds(row["conds"])
        df = R
        for (a, v) in conds:
            df = df.filter(col(a) == float(v))
        df = df.select("row_id").withColumn("q_id", lit(qid))
        hits = df if hits is None else hits.unionByName(df)
    if hits is None:
        pop = R.select("row_id").withColumn("pop", lit(0))
    else:
        pop = hits.groupBy("row_id").agg(countDistinct("q_id").alias("pop"))
    return R.join(pop, on="row_id", how="left").fillna({"pop": 0})

# =============================
# Standardization
# =============================
def standardize(df, cols):
    stats = df.select(
        *[avg(c).alias(f"{c}_mean") for c in cols],
        *[stddev(c).alias(f"{c}_std") for c in cols]
    ).collect()[0].asDict()
    out = df
    for c in cols:
        mu = float(stats.get(f"{c}_mean", 0.0) or 0.0)
        sd = float(stats.get(f"{c}_std", 1.0) or 1.0)
        if sd == 0:
            sd = 1.0
        out = out.withColumn(c, (col(c) - lit(mu)) / lit(sd))
    return out

# =============================
# Main Function
# =============================
def main():
    spark = get_spark()
    print("✅ Spark session initialized.")
    
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--threshold", type=int, required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--distance", default="euclidean", choices=["euclidean", "cosine"])
    ap.add_argument("--standardize", action="store_true")
    ap.add_argument("--sample_rate", type=float, default=1.0)
    args = ap.parse_args()

    t0 = time.time()

    # === Step 1. Load dataset ===
    R = spark.read.option("header", "true").csv(args.input)
    if "row_id" not in R.columns:
        R = R.withColumn("row_id", monotonically_increasing_id())
    feature_cols = [c for c in R.columns if c != "row_id"]
    for c in feature_cols:
        R = R.withColumn(c, col(c).cast("double"))

    t1 = time.time()

    # === Step 2. Popularity ===
    Rp = compute_popularity(spark, R, args.queries).cache()
    t2 = time.time()

    # === Step 3. Standardization ===
    if args.standardize:
        Rp = standardize(Rp, feature_cols)

    # === Step 4. Convert to Pandas for exact computation ===
    pdf = Rp.toPandas()
    n = len(pdf)
    print(f"Loaded {n} tuples. Computing pairwise distances...")

    # === Step 5. Optional sampling to avoid OOM ===
    if args.sample_rate < 1.0:
        pdf = pdf.sample(frac=args.sample_rate, random_state=42)
        n = len(pdf)
        print(f"Sampled {n} tuples (sample_rate={args.sample_rate}).")

    mat = pdf[feature_cols].to_numpy(dtype=float)
    pop = pdf["pop"].to_numpy(dtype=float)
    imp_vals = np.zeros(n)

    # === Step 6. Distance computation ===
    if args.distance == "euclidean":
        for i in range(n):
            diffs = np.sqrt(np.sum((mat[i] - mat) ** 2, axis=1))
            avg_diff = np.mean(diffs[diffs != 0])
            imp_vals[i] = pop[i] * avg_diff
            if (i + 1) % 50 == 0 or i == n - 1:
                print(f"Computed importance for {i+1}/{n}")
    else:  # cosine
        norms = np.linalg.norm(mat, axis=1)
        for i in range(n):
            sims = np.dot(mat[i], mat.T) / (norms[i] * norms + 1e-8)
            diffs = 1 - sims
            avg_diff = np.mean(diffs[diffs != 0])
            imp_vals[i] = pop[i] * avg_diff
            if (i + 1) % 50 == 0 or i == n - 1:
                print(f"Computed importance for {i+1}/{n}")

    pdf["importance"] = imp_vals

    # === Step 7. Select top T ===
    pdf_sorted = pdf.sort_values(by="importance", ascending=False).head(args.threshold)
    print(f"Selected top-{len(pdf_sorted)} tuples.")
    t3 = time.time()
    
    # === Step 8. Save results ===
    spark.createDataFrame(pdf_sorted).coalesce(1).write.mode("overwrite").option("header", "true").csv(args.output)
    print(f"[Exact] Method 2 wrote {len(pdf_sorted)} tuples to {args.output}")
    print(f"timings(s): load={t1-t0:.2f}, pop={t2-t1:.2f}, compute={t3-t2:.2f}, total={t3-t0:.2f}")
    t4 = time.time()
    # ---------- Summary ----------
    print(f"✅ Method 2 finished: wrote top-{args.threshold} tuples to {args.output}")
    print(f"Timings (s): load={t1-t0:.2f}, pop={t2-t1:.2f}, select={t3-t2:.2f}, write={t4-t3:.2f}, total={t4-t0:.2f}")

    # (Optional) 保存到日志文件，方便 plots.py 读取
    with open("timings_summary.csv", "a") as f:
        f.write(f"method2,{args.input},{args.threshold},{t1-t0:.2f},{t2-t1:.2f},{t3-t2:.2f},{t4-t3:.2f},{t4-t0:.2f}\n")

    spark.stop()

if __name__ == "__main__":
    main()
