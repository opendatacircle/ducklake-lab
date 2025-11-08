# DuckLake Lab

**DuckLake Lab** is a collection of hands-on demos and experiments showcasing how to use **DuckDB**, **DuckLake**, and integrations with **Apache Spark** and other modern data tools.

Each demo is self-contained and designed to help you learn and explore step-by-step, from basic queries to advanced lakehouse integrations.

---

## 🚀 Demos

| Demo                                                           | Description                                         |
| -------------------------------------------------------------- | --------------------------------------------------- |
| [duckdb-basics](demos/duckdb-basics)                           | Learn DuckDB fundamentals and local analytics.      |
| [ducklake-intro](demos/ducklake-intro)                         | Explore DuckLake setup and data ingestion.          |
| [spark-ducklake-integration](demos/spark-ducklake-integration) | Connect Spark to DuckLake and run hybrid workloads. |

> Each demo includes its own `README.md` explaining setup, dependencies, and execution.


## 🧩 Adding a New Demo

To add a new demo:
1. Create a new folder under demos/, e.g.: `demos/<new-demo-name>/`
2. Add: 
   - `README.md` — describing purpose, setup, and usage
   - Any code, notebooks, or configs needed
3. Update the demo list above in this README.

Each demo should be independent — runnable without relying on others.

## About
DuckLake Lab is built for experimentation and learning — from local DuckDB analytics to modern lakehouse integrations with Spark and beyond.
