#!/usr/bin/env python3
# Sarah Chambers

import argparse, os, glob, shutil, time

def save_one_csv(df, path):
    """Write a small Spark DataFrame to a single CSV file (header included)."""
    from pyspark.sql import DataFrame
    assert isinstance(df, DataFrame)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if df.rdd.isEmpty():
        with open(path, "w", encoding="utf-8") as f:
            f.write(",".join(df.columns) + "\n")
        print(f"[save_one_csv] Wrote empty CSV with header -> {path}")
        return

    tmp = path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
    time.sleep(0.8)  # small delay to avoid eventual consistency hiccups
    parts = sorted(glob.glob(os.path.join(tmp, "part-*")))
    if not parts:
        raise RuntimeError(f"No part-* produced in {tmp}")
    shutil.move(parts[0], path)
    shutil.rmtree(tmp, ignore_errors=True)

def make_plots(timeline_csv, cluster_csv, outdir):
    import pandas as pd
    import matplotlib.pyplot as plt

    t = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
    c = pd.read_csv(cluster_csv)

    # bar chart
    plt.figure()
    x = c["cluster_id"].astype(str)
    y = c["num_applications"].astype(int)
    bars = plt.bar(x, y)
    for b in bars:
        plt.text(b.get_x() + b.get_width()/2, b.get_height(),
                 str(int(b.get_height())), ha="center", va="bottom")
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "problem2_bar_chart.png"), dpi=150)
    plt.close()

    top_row = c.sort_values("num_applications", ascending=False).iloc[0]
    top_id = str(top_row["cluster_id"])
    t1 = t[t["cluster_id"].astype(str) == top_id].copy()
    t1["dur_min"] = (t1["end_time"] - t1["start_time"]).dt.total_seconds() / 60.0

    plt.figure()
    plt.hist(t1["dur_min"], bins=30)
    plt.xscale("log")
    plt.xlabel("Job Duration (minutes, log scale)")
    plt.ylabel("Count")
    plt.title(f"Duration Distribution — Cluster {top_id} (n={len(t1)})")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "problem2_density_plot.png"), dpi=150)
    plt.close()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("master", nargs="?")                  
    p.add_argument("--net-id", default=None)
    p.add_argument("--in", dest="inp", default=None)
    p.add_argument("--outdir", default="/home/ubuntu/spark-cluster")
    p.add_argument("--skip-spark", action="store_true")
    a = p.parse_args()

    os.makedirs(a.outdir, exist_ok=True)
    tl_path = os.path.join(a.outdir, "problem2_timeline.csv")
    cs_path = os.path.join(a.outdir, "problem2_cluster_summary.csv")
    st_path = os.path.join(a.outdir, "problem2_stats.txt")

    if not a.skip_spark:
        if not a.master:
            raise SystemExit("Error: master URL required unless --skip-spark is set.")
        if not a.net_id and not a.inp:
            raise SystemExit("Error: --net-id (or --in to override input path) is required.")

        src = a.inp or f"s3a://{a.net_id}-assignment-spark-cluster-logs/data/"

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            col, regexp_extract, input_file_name,
            to_timestamp, coalesce, min as smin, max as smax, count as scount
        )

        spark = (
            SparkSession.builder.master(a.master)
            .appName("P2-ClusterUsage")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Read logs (recursive)
        df = (
            spark.read.format("text")
            .option("recursiveFileLookup", "true")
            .load(src.rstrip("/") + "/")
            .select(col("value").alias("line"), input_file_name().alias("path"))
        )

        # Extract IDs
        df = df.withColumn("application_id", regexp_extract(col("path"), r"(application_\d+_\d+)", 1))
        df = df.withColumn("cluster_id",    regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1))
        df = df.withColumn("app_number",    regexp_extract(col("application_id"), r"application_\d+_(\d+)", 1))

        # Extract timestamps (supports yy/MM/dd and yyyy-MM-dd, with/without millis)
        ts_re = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)"
        df = df.withColumn("ts_str", regexp_extract(col("line"), ts_re, 1))
        df = df.withColumn(
            "ts",
            coalesce(
                to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss,SSS"),
                to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss"),
                to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss,SSS"),
                to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"),
            )
        )

        df = df.filter((col("application_id") != "") & col("ts").isNotNull())

        timeline = (
            df.groupBy("cluster_id", "application_id", "app_number")
              .agg(smin("ts").alias("start_time"), smax("ts").alias("end_time"))
              .orderBy("cluster_id", "app_number")
        )

        # Save with exact column names/order required
        save_one_csv(
            timeline.select("cluster_id", "application_id", "app_number", "start_time", "end_time"),
            tl_path
        )

        # Cluster summary
        cluster_summary = (
            timeline.groupBy("cluster_id")
                    .agg(
                        scount("*").alias("num_applications"),
                        smin("start_time").alias("cluster_first_app"),
                        smax("end_time").alias("cluster_last_app"),
                    )
                    .orderBy(col("num_applications").desc())
        )
        save_one_csv(cluster_summary, cs_path)

        # Stats text
        nc = cluster_summary.count()
        na = timeline.count()
        avg = (na / nc) if nc else 0.0
        top5 = cluster_summary.limit(5).collect()
        with open(st_path, "w", encoding="utf-8") as f:
            f.write(f"Total unique clusters: {nc}\n")
            f.write(f"Total applications: {na}\n")
            f.write(f"Average applications per cluster: {avg:.2f}\n\n")
            f.write("Most heavily used clusters:\n")
            for r in top5:
                f.write(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications\n")

        spark.stop()

    make_plots(tl_path, cs_path, a.outdir)
    print("Wrote:")
    print(" ", tl_path)
    print(" ", cs_path)
    print(" ", st_path)
    print(" ", os.path.join(a.outdir, "problem2_bar_chart.png"))
    print(" ", os.path.join(a.outdir, "problem2_density_plot.png"))

if __name__ == "__main__":
    main()

