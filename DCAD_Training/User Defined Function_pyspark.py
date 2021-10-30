# Databricks notebook source
# MAGIC %run ./Create_DataFrames_pyspark

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def string_concat(sep,first,second):
    return first + sep + second

# COMMAND ----------

string_concat("_","hello","world")

# COMMAND ----------

# MAGIC %md
# MAGIC Register the function as DataFrame function

# COMMAND ----------

string_concat_udf = udf(lambda sep,first,second: string_concat(sep,first,second), StringType())

# COMMAND ----------

customerDf.na.drop("any").select(col("firstname"),col("lastname"),string_concat_udf(lit(" "),'firstname','lastname').alias("fullName")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC register the function as SQL function

# COMMAND ----------

spark.udf.register("SQL_StringConcat_udf",string_concat, StringType())

# COMMAND ----------

customerDf.na.drop("any").selectExpr("firstname","lastname","SQL_StringConcat_udf('->',firstname,lastname)").show()

# COMMAND ----------

customerDf.write.saveAsTable("customer")

# COMMAND ----------

spark.sql("""
     select firstname, lastname,SQL_StringConcat_udf(' ',firstname,lastname) as fullName
     from customer
     where firstname is not null
     and lastname is not null""").show()