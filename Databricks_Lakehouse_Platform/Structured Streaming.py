# Databricks notebook source
# MAGIC %md
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Workspace/Repos/donald.modiba@altron.com/Databricks-Certified-Data-Engineer-Associate/Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream.table("books").createOrReplaceTempView("books_streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into books
# MAGIC Values("Test2", "Test2", "Test2","Test2",560)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_count_tmp_vw AS (
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author
# MAGIC );

# COMMAND ----------

(spark.table("author_count_tmp_vw").writeStream.trigger(processingTime='4 seconds').outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint1").table("author_counts"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

# MAGIC  %sql
# MAGIC INSERT INTO books
# MAGIC VALUES ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_count_tmp_vw").writeStream.trigger(availableNow=True).outputMode("complete").option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint1").table("author_counts").awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("T00", "Test1", "Test1", "Test1", 25)
