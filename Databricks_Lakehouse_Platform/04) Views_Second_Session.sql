-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;

DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES in global_temp;
