#!/usr/bin/env python3
# Method 1: importance == popularity; pick top-T tuples by pop(t).
import argparse, csv
from pyspark.sql import SparkSession
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
            a,v = tok.split("=",1)
            parts.append((a.strip(), v.strip()))
    return parts

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=str, required=True, help="CSV path of R")
    ap.add_argument("--queries", type=str, required=True, help="CSV path of queries")
    ap.add_argument("--threshold", type=int, required=True, help="T")
    ap.add_argument("--output", type=str, required=True, help="output CSV path")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("method1_popularity").getOrCreate()

    R = spark.read.option("header","true").csv(args.input)
    if "row_id" not in R.columns:
        # if not given, attach row_id
        R = R.withColumn("row_id", monotonically_increasing_id())

    # Ensure numeric columns are cast to double for comparisons
    numeric_cols = [c for c in R.columns if c != "row_id"]
    for c in numeric_cols:
        R = R.withColumn(c, col(c).cast("double"))

    Q = spark.read.option("header","true").csv(args.queries)
    # Evaluate queries by filtering R per query; union results with q_id, then count distinct q_id per row_id
    # NOTE: This is scan-heavy if |Q| is large; for large |Q|, implement inverted index (TODO).
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
        # no queries, popularity 0
        scored = R.withColumn("pop", lit(0.0))
    else:
        pop = hits.groupBy("row_id").agg(countDistinct("q_id").alias("pop"))
        scored = R.join(pop, on="row_id", how="left").fillna({"pop":0})

    # pick top-T by pop
    topT = scored.orderBy(col("pop").desc()).limit(args.threshold)
    topT.coalesce(1).write.mode("overwrite").option("header","true").csv(args.output)

    print(f"Method 1 wrote top-{args.threshold} tuples to {args.output}")
    spark.stop()

if __name__ == "__main__":
    main()
