# Databricks notebook source
print('Hello World!')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Subtitle 1
# MAGIC ### SubSubtitle 1
# MAGIC
# MAGIC Text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Order List
# MAGIC 1. One
# MAGIC 1. Two
# MAGIC 1. Three
# MAGIC
# MAGIC Unodered List
# MAGIC * Apples
# MAGIC * Peaches
# MAGIC * Bananas
# MAGIC
# MAGIC Sec Unodered List
# MAGIC - Apples
# MAGIC - Peaches
# MAGIC - Bananas
# MAGIC
# MAGIC Images:
# MAGIC ![Something](https://images.ctfassets.net/hrltx12pl8hq/28ECAQiPJZ78hxatLTa7Ts/2f695d869736ae3b0de3e56ceaca3958/free-nature-images.jpg?fit=fill&w=1200&h=630)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    Don    |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://github.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate" target="_blank">Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %run 
# MAGIC ./Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

display(files)

# COMMAND ----------


