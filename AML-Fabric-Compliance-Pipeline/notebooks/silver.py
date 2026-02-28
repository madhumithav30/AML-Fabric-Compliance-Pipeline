#  Silver Layer -- SCD 2 
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid

# --- 1. CONFIGURATION ---
target_table = "customer_registry"
quarantine_table = "quarantine_bad_records"
load_id = str(uuid.uuid4())
source_system = "TAMIL_NADU_CORE_BANKING"
end_of_time = "9999-12-31 23:59:59"
approved_districts = ["Chennai", "Coimbatore", "Madurai", "Trichy", "Salem", "Tirunelveli"]

# --- 2. SCHEMA RESET (Run once if you hit schema errors) ---
if spark.catalog.tableExists(target_table):
    existing_cols = spark.read.table(target_table).columns
    if "row_hash" not in existing_cols:
        spark.sql(f"DROP TABLE {target_table}")

# --- 3. THE  DATA QUALITY GATE (In case of bad records -- quarantine them) ---
df_bronze = spark.read.table("bronze_upi_txns")

# Check multiple rules and store the reason if it fails
df_validated = df_bronze.withColumn("error_reason", 
    F.when(F.col("Amount") <= 0, "Invalid Amount: Non-positive")
     .when(F.col("Amount") > 1000000, "Invalid Amount: Exceeds 10L Limit")
     .when(~F.col("UPI_ID").rlike("^[a-zA-Z0-9.-]+@[a-zA-Z0-9.-]+$"), "Format Error: Invalid UPI ID")
     .when(~F.col("District").isin(approved_districts), "Logic Error: District not in TN master list")
     .otherwise(F.lit(None))
)

# SPLIT DATA: Good records vs Bad records
df_good_raw = df_validated.filter("error_reason IS NULL").drop("error_reason")
df_bad = df_validated.filter("error_reason IS NOT NULL") \
                     .withColumn("Load_ID", F.lit(load_id)) \
                     .withColumn("Rejected_At", F.current_timestamp())

# SAVE BAD RECORDS: This is your Dead Letter Queue (DLQ)
if df_bad.count() > 0:
    print(f"⚠️ {df_bad.count()} bad records found. Routing to {quarantine_table}...")
    df_bad.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)

# --- 4. DEDUPLICATE GOOD RECORDS (Rank Logic) ---
# Ensuring Accuracy: Taking only the latest record if duplicates exist in source
window_latest = Window.partitionBy("UPI_ID").orderBy(F.col("Timestamp").desc())
df_source_clean = df_good_raw.withColumn("rank", F.row_number().over(window_latest)) \
                             .filter("rank = 1").drop("rank")

# Generate Row Hash for change detection
change_cols = ["District", "MerchantCategory", "Amount"]
df_source_final = df_source_clean.withColumn("row_hash", F.md5(F.concat_ws("|", *change_cols))) \
                                 .withColumn("Source_System", F.lit(source_system)) \
                                 .withColumn("Load_ID", F.lit(load_id))

# --- 5. THE SCD2 MERGE (Incremental Sync) ---
if not spark.catalog.tableExists(target_table):
    # INITIAL LOAD
    df_source_final.withColumn("Effective_Start", F.current_timestamp()) \
                   .withColumn("Effective_End", F.to_timestamp(F.lit(end_of_time))) \
                   .withColumn("Is_active", F.lit(True)) \
                   .withColumn("Is_Deleted", F.lit(False)) \
                   .write.format("delta").option("delta.enableDeletionVectors", "true").saveAsTable(target_table)
else:
    # INCREMENTAL UPSERT
    target_delta = DeltaTable.forName(spark, target_table)
    df_target_current = spark.read.table(target_table).filter("Is_active = true")
    
    staged_updates = df_source_final.alias("src").join(df_target_current.alias("tgt"), "UPI_ID") \
        .filter("src.row_hash <> tgt.row_hash").select("src.*")

    upsert_df = df_source_final.withColumn("merge_key", F.col("UPI_ID")) \
        .unionByName(staged_updates.withColumn("merge_key", F.lit(None).cast("string")))

    target_delta.alias("tgt").merge(upsert_df.alias("src"), "tgt.UPI_ID = src.merge_key") \
    .whenMatchedUpdate(
        condition = "tgt.Is_active = true AND tgt.row_hash <> src.row_hash",
        set = {"Is_active": "false", "Effective_End": F.current_timestamp()}
    ).whenNotMatchedInsert(
        values = {
            "UPI_ID": "src.UPI_ID", "AadhaarMasked": "src.AadhaarMasked", "Amount": "src.Amount",
            "District": "src.District", "MerchantCategory": "src.MerchantCategory",
            "Timestamp": "src.Timestamp", "row_hash": "src.row_hash", 
            "Source_System": "src.Source_System", "Load_ID": "src.Load_ID",
            "Effective_Start": F.current_timestamp(), "Effective_End": F.to_timestamp(F.lit(end_of_time)),
            "Is_active": "true", "Is_Deleted": "false"
        }
    ).execute()

# --- 6. HANDLE SOFT DELETES ---
target_delta = DeltaTable.forName(spark, target_table)
target_delta.alias("tgt").merge(df_source_clean.alias("src"), "tgt.UPI_ID = src.UPI_ID") \
    .whenNotMatchedBySourceUpdate(
        condition = "tgt.Is_active = true",
        set = {"Is_active": "false", "Effective_End": F.current_timestamp(), "Is_Deleted": "true"}
    ).execute()

print(f"SCD2 Sync Successful. Records Quarantined: {df_bad.count()}. LoadID: {load_id}")