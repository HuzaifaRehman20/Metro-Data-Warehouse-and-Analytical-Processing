# METRO Near-Real-Time Data Warehouse System

## 📌 Project Overview

This project is a prototype of a **near-real-time Data Warehouse (DW)** designed for **METRO**, a leading superstore chain in Pakistan. It aims to analyze customer shopping behavior to optimize sales strategies using real-time data ingestion and enrichment.

The system simulates a real-world ETL (Extract, Transform, Load) pipeline using Java, employing the **MESHJOIN algorithm** to handle streaming customer transactions and master data joins.

---

## 🛠️ Key Features

- ✅ Real-time ETL with stream-relation join
- ✅ Implementation of **MESHJOIN** in Java (Eclipse IDE)
- ✅ Star-schema-based Data Warehouse design
- ✅ Enrichment of transactional data using master data
- ✅ In-depth DW analytics using OLAP queries

---

## 🧱 Data Warehouse Design

The DW follows a **Star Schema** model:

- **Fact Table:** Transactional Sales
- **Dimension Tables:**
  - Customers (Customer ID, Name, Gender)
  - Products (Product ID, Name, Price, Supplier ID)
  - Suppliers (Supplier ID, Name)
  - Stores (Store ID, Name)
  - Time (Order Date)

**Derived Attribute:** `TOTAL_SALE = QUANTITY × PRODUCT_PRICE`

---

## 🔄 ETL: Extended MESHJOIN Implementation

The MESHJOIN-based ETL pipeline includes:

1. **Stream Ingestion:** Read chunks of customer transactions into a hash table and queue.
2. **Master Data Load:** Load cyclic MD partitions (Customers, Products) into disk buffers.
3. **Join Operation:** Join MD and transactional streams to enrich transaction data.
4. **Enrichment:** Add missing attributes to transactions.
5. **Load to DW:** Insert into DW while avoiding duplicate dimension records.
6. **Repeat:** Until all data is loaded.

---

## 📊 OLAP Queries Implemented

1. **Top Revenue Products (Weekday vs Weekend)** – Drill-down by month
2. **Quarterly Store Revenue Growth (2017)**
3. **Supplier Sales by Store and Product**
4. **Seasonal Product Sales Trends**
5. **Revenue Volatility by Store/Supplier**
6. **Product Affinity Analysis (Top 5 bundles)**
7. **ROLLUP: Revenue Trends by Store → Supplier → Product**
8. **H1 vs H2 Sales Analysis**
9. **Outlier Detection in Daily Sales**
10. **Materialized View: `STORE_QUARTERLY_SALES`**

---

## 🧩 Task Breakdown

- [x] Design star schema and define tables
- [x] Implement MESHJOIN algorithm in Java
- [x] Load and enrich transaction data in DW
- [x] Perform OLAP analysis using slicing, dicing, drill-down
- [x] Document project report with:
  - Project overview
  - DW Schema
  - Mesh Join explanation
  - 3 limitations of Mesh Join
  - Learning outcomes

---

## 📝 Technologies Used

- 💻 Java (Eclipse IDE)
- 🗃️ Relational DB for DW (MySQL)
- 🧠 OLAP Querying (SQL with slicing, dicing, roll-up, drill-down)
- 🖼️ Star Schema modeling

---

## 📘 What I Learned

- Implementation of real-time ETL pipelines
- Stream-relation join mechanics using MESHJOIN
- Star schema design and OLAP-based decision analytics
- Data enrichment, schema modeling, and performance optimization
