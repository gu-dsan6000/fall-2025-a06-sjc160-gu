#!/usr/bin/env python3
# Sarah Chambers

import argparse, os, csv, glob, shutil, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, input_file_name,
    to_timestamp, coalesce,
    min as smin, max as smax, count as scount
)

def save_one_csv(df, path):
    # write a single csv by coalescing and moving the part file
    tmp = path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
    time.sleep(1)  # small wait helps some filesystems
    parts = sorted(glob.glob(os.path.join(tmp, "part-*")))
    if not parts:
        # show folder to help debug
        try:
            print("debug tmp listing:", os.listdir(tmp))
        except Exception as e:
            print("debug could not list tmp:", e)
        raise RuntimeError(f"no part-* file found in {tmp}")
    shutil.move(parts[0], path)
    shutil.rmtree(tmp, ignore_errors=True)

def make_plots(timeline_csv, cluster_csv, outdir):
    import pandas as pd
    import matplotlib.pyplot as plt

    t = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
    c = pd.read_csv(cluster_csv)

    # bar chart for number of apps per cluster
    plt.figure()
    bars = plt.bar(c["cluster_id"].astype(str), c["num_apps"])
    for b in bars:
        plt.text(b.get_x() + b.get_width()/2, b.get_height(),
                 str(int(b.get_height())), ha="center", va="bottom")
    plt.title("apps per cluster")
    plt.xlabel("cluster id")
    plt.ylabel("apps")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "problem2_bar_chart.png"), dpi=150)
    plt.close()

    # histogram for duration of top cluster
    top = c.sort_values("num_apps", ascending=False).iloc[0]["cluster_id"]
    t1 = t[t["cluster_id"] == top].copy()
    t1["dur_min"] = (t1["end_time"] - t1["start_time"]).dt.total_seconds() / 60
    plt.figure()
    plt.hist(t1["dur_min"], bins=30)
    plt.xscale("log")
    plt.xlabel("duration (min, log)")
    plt.ylabel("count")
    plt.title(f"durations for cluster {int(top)} (n={len(t1)})")
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
        src = a.inp or f"s3a://{a.net_id}-assignment-spark-cluster-logs/data/"
        spark = (
            SparkSession.builder.master(a.master)
            .appName("p2")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # read logs
        df = (
            spark.read.format("text")
            .option("recursiveFileLookup", "true")
            .load(src.rstrip("/") + "/")
            .select(col("value").alias("line"), input_file_name().alias("path"))
        )

        # extract ids and times
        df = df.withColumn("app_id",     regexp_extract(col("path"),   r"(application_\d+_\d+)", 1))
        df = df.withColumn("cluster_id", regexp_extract(col("app_id"), r"application_(\d+)_\d+", 1))
        df = df.withColumn("app_num",    regexp_extract(col("app_id"), r"application_\d+_(\d+)", 1))

# timestamps: match either 17/06/10 15:11:55 or 2017-06-10 15:11:55
        ts_re = r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
        df = df.withColumn("ts_str", regexp_extract(col("line"), ts_re, 1))
        df = df.withColumn(
            "ts",
            expr("coalesce(to_timestamp(ts_str, 'yy/MM/dd HH:mm:ss'), to_timestamp(ts_str, 'yyyy-MM-dd HH:mm:ss'))")
        )
        df = df.filter((col("app_id") != "") & col("ts").isNotNull())


        # timeline: one row per app with start/end
        t = (
            df.groupBy("cluster_id", "app_id", "app_num")
              .agg(smin("ts").alias("start_time"), smax("ts").alias("end_time"))
              .orderBy("cluster_id", "app_num")
        )
        save_one_csv(t, tl_path)

        # cluster summary: how many apps and overall window
        c = (
            t.groupBy("cluster_id")
             .agg(
                 scount("*").alias("num_apps"),
                 smin("start_time").alias("first_app"),
                 smax("end_time").alias("last_app"),
             )
             .orderBy(col("num_apps").desc())
        )
        save_one_csv(c, cs_path)

        # write stats
        nc = c.count()
        na = t.count()
        avg = na / nc if nc else 0
        top = c.limit(5).collect()
        with open(st_path, "w") as f:
            f.write(f"clusters: {nc}\napps: {na}\navg apps per cluster: {avg:.2f}\n\n")
            for r in top:
                f.write(f"cluster {r['cluster_id']}: {r['num_apps']} apps\n")

        spark.stop()

    # make charts
    make_plots(tl_path, cs_path, a.outdir)
    print("done, files in", a.outdir)

if __name__ == "__main__":
    main()

