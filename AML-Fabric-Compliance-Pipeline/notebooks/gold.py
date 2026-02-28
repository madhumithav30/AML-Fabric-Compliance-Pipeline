# Gold LAYER 

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# --- 1. SOURCE SELECTION  ----
# We use the Silver table because it has the validated UPI IDs and the 'Is_active' flag
df_silver = spark.read.table("customer_registry").filter("Is_active = true")

# --- 2. FRAUD DETECTION LOGIC (Smurfing Pattern) ---
# 1-hour sliding window: Looks back 3600 seconds from each transaction
window_spec = Window.partitionBy("UPI_ID").orderBy(F.col("Timestamp").cast("long")).rangeBetween(-3600, 0)

# We use 'UPI_ID' as the grouping key for Tamil Nadu UPI fraud detection
smurfing_analysis = df_silver.withColumn("Txn_Count_1h", F.count("UPI_ID").over(window_spec)) \
                             .withColumn("Total_Amt_1h", F.sum("Amount").over(window_spec))

# Filter for suspicious patterns (PMLA High Risk: > 5 txns in 1hr AND total > 50,000)
suspicious_activity = smurfing_analysis.filter(
    (F.col("Txn_Count_1h") >= 5) & 
    (F.col("Total_Amt_1h") > 50000)
)

# --- 3. HANDLE  SCHEMA MISMATCH ---
# .option("overwriteSchema", "true") tells Delta to drop the old column definitions 
# and adopt the new ones from our updated Silver-to-Gold logic.
suspicious_activity.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_pmla_alerts")

# --- 4. PERFORMANCE OPTIMIZATION ---
# Re-applying Z-ORDER so the Power BI Map is lightning fast for Tamil Nadu districts
spark.sql("OPTIMIZE gold_pmla_alerts ZORDER BY (District)")

print("Gold Table Created Successfully: Schema has been updated to Advanced PMLA format.")