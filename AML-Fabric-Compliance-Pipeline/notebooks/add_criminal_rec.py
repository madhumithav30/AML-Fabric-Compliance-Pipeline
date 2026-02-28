#  Inject Criminal Records to Gold layer 
from datetime import datetime
from decimal import Decimal
from pyspark.sql import functions as F
import uuid

# 1. Setup
target_table = "gold_pmla_alerts"
load_id = str(uuid.uuid4())

# 2. Get the EXACT schema from the table
target_schema = spark.read.table(target_table).schema

# 3. Create the data rows 
# Ensure every column from your screenshot is represented here in the correct order
# If you missed a column in the list below, the script will tell you
data_rows = []
for i in range(6):
    data_rows.append({
        "TxnID": f"CRIM_TXN_{900+i}",
        "UPI_ID": "criminal_user@oksbi",
        "AadhaarMasked": "XXXX-XXXX-0000",
        "Amount": Decimal("12000.00"),
        "District": "Chennai",
        "MerchantCategory": "Jewellery_Shop",
        "Timestamp": datetime.now(),
        "row_hash": "manual_injection_hash",
        "Source_System": "MANUAL_FIX",
        "Load_ID": load_id,
        "Effective_Start": datetime.now(),
        "Effective_End": datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S"),
        "Is_active": True,
        "Is_Deleted": False,
        "Txn_Count_1h": 6,          # This will be cast to Long
        "Total_Amt_1h": Decimal("72000.00") # This will be cast to Decimal
    })

# 4. Create the DataFrame using the EXACT schema object from the table
# This forces every column to match the '12L' and '{ }' types from your screenshot
df_manual = spark.createDataFrame(data_rows, schema=target_schema)

# 5. Final Append
df_manual.write.format("delta").mode("append").saveAsTable(target_table)

print(f"SUCCESS: Data injected using the table's native schema object.")