#!/usr/bin/env python3
# Problem 1 — Log Level Distribution
# Sarah Chambers

import argparse, os, csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, upper


def main():
    p = argparse.ArgumentParser()
    p.add_argument("master")                  
    p.add_argument("--net-id", required=True)  
    p.add_argument("--in", dest="inp", default=None)  
    p.add_argument("--outdir", default="/home/ubuntu/spark-cluster")
    args = p.parse_args()

    bucket = f"{args.net_id}-assignment-spark-cluster-logs"
    src = args.inp or f"s3a://{bucket}/data/"

    spark = (
        SparkSession.builder
        .master(args.master)
        .appName("P1-LogLevels")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # read all the logs
    df = (
        spark.read.format("text")
        .option("recursiveFileLookup", "true")
        .load(src.rstrip("/") + "/")
        .select(col("value").alias("line"))
    )

    total_lines = df.count()

    # Extract log level (case-insensitive)
    re_lvl = r"(?i)\b(INFO|WARN|ERROR|DEBUG)\b"
    dfl = df.withColumn("level", regexp_extract(col("line"), re_lvl, 1))
    dfl = dfl.filter(col("level") != "")
    total_with = dfl.count()

    # counts by level
    cnt = (
    dfl.withColumn("level", upper(col("level")))
       .groupBy("level").count() 
    )

    # 10 rand samp
    samp = (
    dfl.withColumn("level", upper(col("level")))
       .orderBy(rand()).limit(10)
       .select("line", "level")
    )

    # collect small outputs to driver and write simple CSV/text files
    os.makedirs(args.outdir, exist_ok=True)
    counts_path  = os.path.join(args.outdir, "problem1_counts.csv")
    sample_path  = os.path.join(args.outdir, "problem1_sample.csv")
    summary_path = os.path.join(args.outdir, "problem1_summary.txt")

    # counts
    counts_rows = {r["level"]: r["count"] for r in cnt.collect()}
    for k in ["INFO", "WARN", "ERROR", "DEBUG"]:
        counts_rows.setdefault(k, 0)

    with open(counts_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_level", "count"])
        for k in ["INFO", "WARN", "ERROR", "DEBUG"]:
            w.writerow([k, counts_rows[k]])

    # sample
    samp_rows = samp.collect()
    with open(sample_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_entry", "log_level"])
        for r in samp_rows:
            w.writerow([r["line"], r["level"]])

    # summary
    def pct(a, b): return 0.0 if b == 0 else 100.0 * a / b
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {total_with:,}\n")
        f.write(f"Unique log levels found: {len([k for k,v in counts_rows.items() if v>0])}\n\n")
        f.write("Log level distribution:\n")
        for k in ["INFO", "WARN", "ERROR", "DEBUG"]:
            c = counts_rows[k]
            f.write(f"  {k:<5}: {c:>10,} ({pct(c,total_with):6.2f}%)\n")

    print("Wrote:")
    print(" ", counts_path)
    print(" ", sample_path)
    print(" ", summary_path)

    spark.stop()

if __name__ == "__main__":
    main()

