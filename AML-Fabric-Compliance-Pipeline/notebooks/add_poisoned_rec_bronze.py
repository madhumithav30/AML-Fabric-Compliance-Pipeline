#  Inject bad records to Bronze layer 
from decimal import Decimal
from datetime import datetime
from pyspark.sql.types import *

# 1. Define the Bronze Schema (Must match your existing Bronze table exactly)
bronze_schema = StructType([
    StructField("TxnID", StringType(), False),
    StructField("UPI_ID", StringType(), False),
    StructField("AadhaarMasked", StringType(), False),
    StructField("Amount", DecimalType(18, 2), False),
    StructField("District", StringType(), False),
    StructField("MerchantCategory", StringType(), False),
    StructField("Timestamp", TimestampType(), False)
])

# 2. Create the "Bad" Data (4 different errors)
bad_data = [
    # ERROR 1: Amount is Negative
    ("BAD_TXN_001", "user1@oksbi", "XXXX-XXXX-1111", Decimal("-500.00"), "Chennai", "Saravana_Stores", datetime.now()),
    
    # ERROR 2: Amount exceeds 10 Lakhs (1,200,000)
    ("BAD_TXN_002", "user2@oksbi", "XXXX-XXXX-2222", Decimal("1200000.00"), "Coimbatore", "Pothys", datetime.now()),
    
    # ERROR 3: Invalid UPI ID Format (missing the @ symbol)
    ("BAD_TXN_003", "invalid_upi_format", "XXXX-XXXX-3333", Decimal("1000.00"), "Madurai", "Aavin", datetime.now()),
    
    # ERROR 4: District not in Tamil Nadu (e.g., Bangalore)
    ("BAD_TXN_004", "user4@oksbi", "XXXX-XXXX-4444", Decimal("5000.00"), "Bangalore", "Retail_Shop", datetime.now())
]

# 3. Create DataFrame
df_bad_records = spark.createDataFrame(bad_data, schema=bronze_schema)

# 4. APPEND to Bronze (This "poisons" the raw layer)
df_bad_records.write.format("delta").mode("append").saveAsTable("bronze_upi_txns")

print("Poisoned Bronze with 4 bad records")