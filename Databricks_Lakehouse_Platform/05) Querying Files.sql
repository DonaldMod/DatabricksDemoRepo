-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Demo/Course-Materials/Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`;

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`;

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT *, input_file_name()
FROM json.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv/`;

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv";

-- COMMAND ----------

SELECT * FROM books_csv;

-- COMMAND ----------

DESC EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark.read.table("books_csv").write.mode("append").format("csv").option("header","true").option("delimiter",";").save(f"{dataset_bookstore}/books-csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*) FROM books_csv;

-- COMMAND ----------

REFRESH TABLE books_csv;

-- COMMAND ----------

SELECT count(*) FROM books_csv;

-- COMMAND ----------

CREATE TABLE customers
AS SELECT * FROM JSON.`${dataset.bookstore}/customers-json/`;

-- COMMAND ----------

DESC EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed
AS SELECT * FROM csv.`${dataset.bookstore}/books-csv/`;

-- COMMAND ----------

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMPORARY VIEW books_tmp_vw
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books
AS SELECT * FROM books_tmp_vw;

-- COMMAND ----------

SELECT * FROM books;

-- COMMAND ----------

DESC EXTENDED books;
