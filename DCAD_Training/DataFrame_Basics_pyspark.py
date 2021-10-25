# Databricks notebook source
customerDf = (spark.read.format("json")
                   .schema(customerDfSchema_ST)
                   #.schema(customerDfSchema_DDL)
                   .load("/FileStore/tables/dcad_data/customer.json"))

# COMMAND ----------

customerDfSchema_DDL = "address_id INT,birth_country STRING,birthdate date,customer_id INT,demographics STRUCT<buy_potential: STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,is_preffered_customer STRING,lastname STRING,salutation STRING"

# COMMAND ----------

from pyspark.sql.types import (StructField, StringType, IntegerType, StructType, FloatType, DateType, ArrayType, )
from pyspark.sql.functions import array, struct
customerDfSchema_ST = (StructType([
StructField("address_id",IntegerType(),True),
                   StructField("birth_country",StringType(),True), 
                   StructField("birthdate",DateType(),True), 
                   StructField("customer_id",IntegerType(),True),
                   StructField("demographics",
                               StructType([
                                       StructField("buy_potential",StringType(),True),
                                       StructField("credit_rating",StringType(),True),
                                       StructField("education_status",StringType(),True),
                                       StructField("income_range",ArrayType(IntegerType(),True),True),
                                       StructField("purchase_estimate",IntegerType(),True),
                                       StructField("vehicle_count",IntegerType(),True)
                               ]),True),  
                   StructField("email_address",StringType(),True),
                   StructField("firstname",StringType(),True),
                   StructField("gender",StringType(),True),
                   StructField("is_preffered_customer",StringType(),True),
                   StructField("lastname",StringType(),True),
                   StructField("salutation",StringType(),True)
]))


# COMMAND ----------

# the method
customerDf.printSchema()

# COMMAND ----------

customerDf.printSchema()

# COMMAND ----------

display(customerDf)

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/dcad_data/customer.json

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/dcad_data