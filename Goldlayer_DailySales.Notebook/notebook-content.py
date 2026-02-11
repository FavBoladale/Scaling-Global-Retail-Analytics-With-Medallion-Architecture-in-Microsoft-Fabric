# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e0d37852-414d-4ec9-96bf-6c2f6607eefd",
# META       "default_lakehouse_name": "GoldLayer",
# META       "default_lakehouse_workspace_id": "d07e7eba-50fd-4111-abc0-d4a103e2896e",
# META       "known_lakehouses": [
# META         {
# META           "id": "e0d37852-414d-4ec9-96bf-6c2f6607eefd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT 
    transaction_date,
    SUM(total_amount) AS daily_total_sales
FROM 
    binaryville.silverLayer.dbo.silver_orders
GROUP BY 
    transaction_date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the data in the Gold table
spark.sql("SELECT * FROM gold_daily_sales LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
