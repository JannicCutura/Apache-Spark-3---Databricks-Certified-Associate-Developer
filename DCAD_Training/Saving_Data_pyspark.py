# Databricks notebook source
# MAGIC %run ./Create_DataFrames_pyspark

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

addressDf.count()

# COMMAND ----------

customerWithAddress = (customerDf
            .na.drop("any")
            .join(addressDf, customerDf["address_id"] == addressDf["address_id"])
            .select('customer_id', 'demographics',
                    concat_ws(" ",'firstname', 'lastname').alias("Name"),
                    addressDf["*"])
                   )

# COMMAND ----------

salesWithItem = (webSalesDf
            .na.drop("any")
            .join(itemDf,itemDf["i_item_sk"] == webSalesDf["ws_item_sk"])
            .select(col("ws_bill_customer_sk").alias("customer_id"),
                    col("ws_ship_addr_sk").alias("ship_address_id"),
                    col("i_product_name").alias("item_name"),
                    trim(col("i_category")).alias("item_category"),
                    col("ws_quantity").alias("quantity"),
                    col("ws_net_paid").alias("net_paid")
                       ))


# COMMAND ----------

customerPurchases = (salesWithItem
            .join(customerWithAddress, salesWithItem["customer_id"] == customerWithAddress["customer_id"])
            .select(customerWithAddress["*"],
                    salesWithItem["*"])
            .drop(salesWithItem["customer_id"]))

# COMMAND ----------

display(customerPurchases)

# COMMAND ----------



# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

(customerPurchases
        .select(col("*"),
               trim(col("item_category")).alias("it"))
        .drop("item_category")
        .withColumnRenamed("it","item_category")
        .repartition(8)
        .write
        .partitionBy("item_category")
        .option("compression","lz4")
        .mode("overwrite") 
        .save("/FileStore/tables/dcad_data/output"))

# COMMAND ----------

customerPurchases.select(length(col("item_category"))).show()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/dcad_data/output/item_category=Books/

# COMMAND ----------

spark.read.parquet("dbfs:/FileStore/tables/dcad_data/output/item_category=Books/")