-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Demo/Course-Materials/Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

SELECT order_id, books, filter(books, i -> i.quantity >= 2) AS multiple_copies
FROM orders;

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT order_id, filter(books, i -> i.quantity >= 2) AS multiple_copies
FROM orders
)
WHERE size(multiple_copies) > 0

-- COMMAND ----------

SELECT order_id, books, transform(books, b -> CAST(b.subtotal * 0.8 AS INT))
AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.",split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) AS domain
FROM customers

-- COMMAND ----------

DESC FUNCTION get_url;

-- COMMAND ----------

DESC FUNCTION EXTENDED get_url;

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE
  WHEN email LIKE "%.com" THEN "Commercial Business"
  WHEN email LIKE "%.org" THEN "Non-profits organization"
  WHEN email LIKE "%.edu" THEN "Educational institution"
  ELSE concat("Unknown extension for domain:   ", split(email,"@")[1] )
 END;

-- COMMAND ----------

SELECT email, site_type(email) AS domain_category
FROM customers;

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------


