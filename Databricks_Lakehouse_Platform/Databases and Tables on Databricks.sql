-- Databricks notebook source
-- Managed table
CREATE TABLE managed_default
(Width INT,
length INT,
height INT  
);

INSERT INTO managed_default
VALUES (3, 2, 1)

-- COMMAND ----------

DESC EXTENDED managed_default

-- COMMAND ----------

-- External Table
CREATE TABLE external_default
(Width INT,
length INT,
height INT  
)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default
VALUES (3, 2, 1);

-- COMMAND ----------

DESC EXTENDED external_default

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESC DATABASE EXTENDED new_default

-- COMMAND ----------

USE new_default;

-- COMMAND ----------

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESC EXTENDED managed_new_default

-- COMMAND ----------

DESC EXTENDED external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default

-- COMMAND ----------

DROP TABLE external_new_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

-- COMMAND ----------

CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schema/custom.db';

-- COMMAND ----------

DESC DATABASE EXTENDED custom;

-- COMMAND ----------

USE custom;

-- COMMAND ----------

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DESC EXTENDED managed_custom

-- COMMAND ----------

DESC EXTENDED external_custom

-- COMMAND ----------

DROP TABLE managed_custom

-- COMMAND ----------

DROP TABLE external_custom

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/Shared/schema/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'
