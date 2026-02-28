# Bronze layer -- Inject Data for UPI transactions 
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Define strict schema
schema = StructType([
    StructField("TxnID", StringType(), False),
    StructField("UPI_ID", StringType(), False),
    StructField("AadhaarMasked", StringType(), False),
    StructField("Amount", DecimalType(18, 2), False),
    StructField("District", StringType(), False),
    StructField("MerchantCategory", StringType(), False),
    StructField("Timestamp", TimestampType(), False)
])

# 2. Data Generation with Tamil Nadu Context
districts = ["Chennai", "Coimbatore", "Madurai", "Trichy", "Salem", "Tirunelveli"]
merchants = ["Saravana Stores", "Pothys", "Aavin", "TNEB_Bill", "Petrol_Bunk", "Jewellery_Shop"]

data = []
for i in range(1, 500):
    # Fix the Decimal error by wrapping the float in Decimal()
    amt = Decimal(str(random.uniform(10.0, 150000.0))).quantize(Decimal('1.00'))
    
    data.append((
        f"UPI_{2024000+i}",
        f"user{random.randint(1,50)}@oksbi",
        f"XXXX-XXXX-{random.randint(1000, 9999)}",
        amt,
        random.choice(districts),
        random.choice(merchants),
        datetime.now() - timedelta(minutes=random.randint(0, 5000))
    ))

df = spark.createDataFrame(data, schema)

# Write to Bronze with V-Order (Fabric's special optimization)
df.write.format("delta").mode("overwrite").option("vorder", "true").saveAsTable("bronze_upi_txns")
print("Bronze Table Created: Raw UPI Transactions for Tamil Nadu.")