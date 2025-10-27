#!/usr/bin/env python3
import argparse, random, csv, math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--cols", type=int, default=6)
    ap.add_argument("--low", type=int, default=0, help="min integer value per cell")
    ap.add_argument("--high", type=int, default=1000, help="max integer value per cell (inclusive)")
    ap.add_argument("--out", type=str, default="Data/synthetic_R.csv")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("generate_dataset").getOrCreate()

    # Create rows of random ints using Spark (so it scales)
    schema_cols = [f"col_{i}" for i in range(args.cols)]
    # Create an RDD of ints
    rdd = spark.sparkContext.parallelize(range(args.rows)).map(
        lambda _: [random.randint(args.low, args.high) for _ in range(args.cols)]
    )
    df = spark.createDataFrame(rdd, schema=schema_cols).withColumn("row_id", monotonically_increasing_id())

    # Reorder to have row_id first (helpful)
    cols = ["row_id"] + schema_cols
    df.select(*cols).coalesce(1).write.mode("overwrite").option("header", "true").csv(args.out)

    print(f"Written dataset to {args.out} (folder with partition files).")

    spark.stop()

if __name__ == "__main__":
    main()
