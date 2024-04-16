-- Databricks notebook source
DESC HISTORY employees

-- COMMAND ----------

SELECT * FROM employees VERSION AS OF 1;

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESC HISTORY employees

-- COMMAND ----------

RESTORE TABLE employees 
TO VERSION AS OF 3;

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESC HISTORY employees

-- COMMAND ----------

DESC DETAIL employees

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id;

-- COMMAND ----------

DESC DETAIL employees

-- COMMAND ----------

DESC HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM employees RETAIN 3 HOURS


-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
