-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Workspace/Repos/donald.modiba@altron.com/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

DESC customers;

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country
FROM customers;

-- COMMAND ----------

SELECT profile
FROM customers
LIMIT 1;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) AS profile_struct
FROM customers;

-- COMMAND ----------

SELECT * FROM parsed_customers;

-- COMMAND ----------

DESC parsed_customers;

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;

-- COMMAND ----------

SELECT * FROM customers_final;

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book 
FROM orders;

-- COMMAND ----------

SELECT customer_id, collect_set(order_id) AS orders_set, collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id;


-- COMMAND ----------

SELECT customer_id, collect_set(books.book_id) AS before_flatten, collect_set(books.book_id) AS after_flatten
FROM orders
GROUP BY customer_id;


-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book
  FROM orders
) o INNER JOIN books b
ON o.book.book_id = b.book_id;

-- COMMAND ----------

SELECT * FROM orders_enriched;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

-- COMMAND ----------

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates;

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates;

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates;

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (
  SELECT customer_id, book.book_id AS book_id, book.quantity AS quantity
  FROM orders_enriched
)PIVOT (
sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
)
);

-- COMMAND ----------

SELECT * FROM transactions;

-- COMMAND ----------


