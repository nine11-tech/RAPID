#!/usr/bin/env python3
"""
Complete Threat Detection Analysis with PySpark
Loads cybersecurity logs, performs aggregation, exports to Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col, round, count, desc

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("ThreatCorrelationAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("="*60)
    print("THREAT DETECTION ANALYSIS WITH PYSPARK")
    print("="*60)
    
    # STEP 1: Load Data
    print("\n[1/6] Loading Cybersecurity Threat Detection Data...")
    
    df = spark.read.csv("hdfs://namenode:9000/data/threat_data.csv", 
                        header=True, 
                        inferSchema=True)
    print("✓ Data loaded successfully from HDFS")
    
    # STEP 2: Schema Verification
    print("\n[2/6] Verifying Data Schema...")
    print("-" * 40)
    df.printSchema()
    
    # STEP 3: Sample Data
    print("\n[3/6] Sample Data (First 5 rows)...")
    print("-" * 40)
    df.show(5, truncate=False)
    
    # STEP 4: Basic Statistics
    print("\n[4/6] Basic Statistics...")
    print("-" * 40)
    total_records = df.count()
    print(f"Total records in dataset: {total_records:,}")
    
    unique_threats = df.select("log_type").distinct().collect()
    print(f"Unique threat types: {len(unique_threats)}")
    for threat in unique_threats:
        print(f"  - {threat[0]}")
    
    # STEP 5: Perform Aggregations
    print("\n[5/6] Computing Aggregations by Threat Type...")
    print("-" * 40)
    
    result_df = df.groupBy("log_type") \
        .agg(
            count("*").alias("occurrence_count"),
            round(avg("bytes_transferred"), 2).alias("avg_bytes_transferred"),
            sum("bytes_transferred").alias("total_bytes_transferred")
        ) \
        .orderBy(col("total_bytes_transferred").desc())
    
    print("\nCORRELATION RESULTS:")
    print("=" * 80)
    result_df.show(truncate=False)
    
    total_bytes_all = df.agg(sum("bytes_transferred")).collect()[0][0]
    result_with_pct = result_df.withColumn(
        "percentage_of_total_traffic",
        round((col("total_bytes_transferred") / total_bytes_all) * 100, 2)
    )
    
    print("\nTRAFFIC DISTRIBUTION WITH PERCENTAGES:")
    print("=" * 80)
    result_with_pct.select(
        "log_type",
        "total_bytes_transferred",
        "percentage_of_total_traffic",
        "occurrence_count",
        "avg_bytes_transferred"
    ).show(truncate=False)
    
    # STEP 6: Export to Parquet
    print("\n[6/6] Exporting Results to Parquet...")
    print("-" * 40)
    
    output_path_hdfs = "hdfs://namenode:9000/results/threat_bytes_correlation.parquet"
    result_df.write.mode("overwrite").parquet(output_path_hdfs)
    print(f"✓ Exported to HDFS: {output_path_hdfs}")
    
    # VERIFICATION
    print("\n" + "=" * 60)
    print("VERIFICATION (Definition of Done)")
    print("=" * 60)
    
    verification_df = spark.read.parquet(output_path_hdfs)
    print("\nVerifying Parquet file contents:")
    verification_df.show(truncate=False)
    
    row_count = verification_df.count()
    print(f"\n✓ Verification successful! {row_count} rows saved to Parquet")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total records processed: {total_records:,}")
    print(f"Total bytes analyzed: {total_bytes_all:,}")
    print(f"Number of threat groups: {row_count}")
    print(f"Output location: {output_path_hdfs}")
    
    print("\n" + "=" * 60)
    print("ALL TASKS COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()