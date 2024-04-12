-- Databricks notebook source
create database if not exists hr_db
location 'dbfs:/mnt/demo/hr_db.db';

USE hr_db;

CREATE TABLE employees (id INT, nname STRING, salary DOUBLE, city STRING);

insert into employees
VALUES (1, "Anna", 2500, "Paris"),
       (2, "Thomas", 3000, "London"),
       (3, "Bilal", 3500, "Paris"),
       (4, "Maya", 2000, "Paris"),
       (5, "Sophie", 2500, "London"),
       (6, "Adam", 3500, "London"),
       (7, "Ali", 3000, "Paris");

CREATE VIEW paris_employees_vw
AS SELECT * from employees WHERE city = 'Paris';


-- COMMAND ----------

GRANT SELECT, MODIFY, READ_METADATA, CREATE ON SCHEMA hr_db TO hr_team;

