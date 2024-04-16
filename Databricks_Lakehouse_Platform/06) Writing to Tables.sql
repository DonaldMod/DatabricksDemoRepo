-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Demo/Course-Materials/Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders
AS SELECT * FROM PARQUET.`${dataset.bookstore}/orders/`

-- COMMAND ----------

SELECT* FROM orders;

-- COMMAND ----------

CREATE OR REPLACE TABLE orders
AS SELECT * FROM PARQUET.`${dataset.bookstore}/orders/`

-- COMMAND ----------

DESC HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders/`;

-- COMMAND ----------

DESC HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() as TimesStam FROM PARQUET.`${dataset.bookstore}/orders/`;

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new/`;

-- COMMAND ----------

SELECT count(*) FROM orders;

-- COMMAND ----------

select * from customers;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW customers_updates AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json-new/`;

-- COMMAND ----------

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
 UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW books_updates
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new/",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

SELECT * FROM books_updates;

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *;
