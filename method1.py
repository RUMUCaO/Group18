#!/usr/bin/env python3
# Method 1: importance == popularity; pick top-T tuples by pop(t).

import os
# 临时绕过 Hadoop 检查
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + os.path.join("C:\\hadoop", "bin")

from pyspark.sql import SparkSession

def get_spark():
    spark = (
        SparkSession.builder
        .appName("method1_popularity")
        .config("spark.driver.memory", "22g")       # driver 内存（控制台那边）
        .config("spark.executor.memory", "8g")      # executor 内存（执行任务）
        .config("spark.sql.shuffle.partitions", "4") # 减少 shuffle 内存负担
        .config("spark.default.parallelism", "4")
        .config("spark.sql.broadcastTimeout", "1200")
        .getOrCreate()
    )
    return spark

import argparse, csv, time
from pyspark.sql.functions import col, lit, monotonically_increasing_id, countDistinct

def parse_conds(s):
    # "col_0=12;col_3=5" -> [("col_0","12"),("col_3","5")]
    if s is None or len(s)==0:
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

def main():
    spark = get_spark()  # 👈 在这里启动 Spark（只运行一次）
    print("✅ Spark session initialized.")
    
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, required=True, help="CSV path of R")
    ap.add_argument("--queries", type=str, required=True, help="CSV path of queries")
    ap.add_argument("--threshold", type=int, required=True, help="T")
    ap.add_argument("--output", type=str, required=True, help="output CSV path")
    args = ap.parse_args()

    t0 = time.time()
    # ---------- Load ----------
    R = spark.read.option("header", "true").csv(args.input)
    if "row_id" not in R.columns:
        R = R.withColumn("row_id", monotonically_increasing_id())

    numeric_cols = [c for c in R.columns if c != "row_id"]
    for c in numeric_cols:
        R = R.withColumn(c, col(c).cast("double"))

    Q = spark.read.option("header", "true").csv(args.queries)
    t1 = time.time()

    # ---------- Compute popularity ----------
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
        scored = R.withColumn("pop", lit(0.0))
    else:
        pop = hits.groupBy("row_id").agg(countDistinct("q_id").alias("pop"))
        scored = R.join(pop, on="row_id", how="left").fillna({"pop": 0})
    t2 = time.time()

    # ---------- Select top-T ----------
    topT = scored.orderBy(col("pop").desc()).limit(args.threshold)
    t3 = time.time()

    # ---------- Write ----------
    # ⚠️ 用 Spark 自带的写法，但确保路径是文件夹
    topT.coalesce(1).write.mode("overwrite").option("header", "true").csv("Data/results/method1_temp")

    # ✅ 然后从那个文件夹里移动生成的 part 文件
    import glob, shutil
    part_file = glob.glob("Data/results/method1_temp/part-*.csv")[0]
    shutil.move(part_file, "Data/results/method1_R10k_T500.csv")
    shutil.rmtree("Data/results/method1_temp")

    t4 = time.time()

    # ---------- Summary ----------
    print(f"✅ Method 1 finished: wrote top-{args.threshold} tuples to {args.output}")
    print(f"Timings (s): load={t1-t0:.2f}, pop={t2-t1:.2f}, select={t3-t2:.2f}, write={t4-t3:.2f}, total={t4-t0:.2f}")

    # (Optional) 保存到日志文件，方便 plots.py 读取
    with open("timings_summary.csv", "a") as f:
        f.write(f"method1,{args.input},{args.threshold},{t1-t0:.2f},{t2-t1:.2f},{t3-t2:.2f},{t4-t3:.2f},{t4-t0:.2f}\n")

    spark.stop()

if __name__ == "__main__":
    main()
