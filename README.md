

# Data Warehouse & Analytics Project

## 📊 Overview

This repository contains an end-to-end **SQL Data Warehouse and Analytics project** built using a modern Medallion architecture (Bronze → Silver → Gold). The goal of the project is to consolidate data from multiple source systems, apply data cleansing and transformation, and deliver an analytics-ready star schema for reporting and business intelligence use cases.

The solution demonstrates practical data engineering and analytics engineering workflows, including data ingestion, transformation, dimensional modeling, and data quality validation.

---

## 🎯 Objective

To design and implement a scalable SQL-based data warehouse that integrates ERP and CRM source data into a unified analytical model, enabling reliable reporting and insight generation.

---

## 🏗️ Architecture

The warehouse follows a layered Medallion design:

* **Bronze Layer** — Raw source data loaded from CSV/source extracts
* **Silver Layer** — Cleaned and standardized datasets with resolved data quality issues
* **Gold Layer** — Business-ready star schema with dimension and fact views for analytics

---

## ⚙️ What This Project Demonstrates

* Data warehouse schema design
* ETL/ELT SQL transformations
* Dimension and fact table modeling
* Surrogate key generation
* Data cleansing and standardization
* Cross-source data integration
* Data quality and referential integrity checks
* Analytics-ready view creation

---

## 🧩 Core Components

* Customer dimension build with enrichment and fallback logic
* Product dimension with category mapping
* Sales fact table linked via surrogate keys
* Validation queries for uniqueness and foreign key integrity
* Documentation-driven SQL scripts for maintainability

---

## 📌 Use Cases

This project can be used as:

* A **portfolio project** for data engineering / analytics roles
* A **reference implementation** for SQL warehouse design
* A **learning resource** for dimensional modeling and ETL patterns
* A base for BI dashboards and reporting models

---

## 🛠️ Tech Stack

* SQL Server / T-SQL
* Dimensional Modeling (Star Schema)
* Medallion Architecture
* GitHub for version control and documentation

---

## 🚀 Outcome

The final Gold layer delivers a clean, analytics-ready data model that supports reliable querying, reporting, and downstream BI tools.
