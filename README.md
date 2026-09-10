# 🚕 Uber End-to-End Real-Time Data Engineering Project

> An end-to-end streaming data engineering solution simulating large-scale ride-booking event telemetry using Azure Cloud, Apache Spark, and a metadata-driven Medallion Architecture.

---

## 📌 Project Overview

This project implements a scalable, production-grade **Streaming Data Platform** simulating an enterprise ride-booking ecosystem (modeled after Uber). The solution combines continuous real-time event streaming with automated batch metadata pipelines, ingesting telemetry data via **Azure Event Hubs**, orchestrating ingestion workflows using **Azure Data Factory (ADF)**, and transforming event streams with **Spark Structured Streaming on Azure Databricks**. 

The system implements an automated **Medallion Architecture (Bronze → Silver → Gold)**, culminating in an optimized dimensional **Star Schema** (1 Fact table, 6 Dimension tables) designed for high-concurrency analytical queries and business intelligence dashboards.

---

## 🎯 Architecture & Engineering Highlights

- **Event Producer & Mock Telemetry Engine:**
  A modular **FastAPI** service generates and emits realistic ride-booking event streams (trip reservations, driver matches, GPS milestones, payment confirmations) with configurable throughput, error simulation, and network latency.

- **Fault-Tolerant Pub/Sub Messaging:**
  Leverages **Azure Event Hubs** using Kafka-compatible APIs to provide distributed, decoupled, and partition-scalable event streaming between producers and downstream consumers.

- **Hybrid Data Ingestion Strategy:**
  - **Streaming Path:** Real-time event streams ingested directly into streaming Delta tables.
  - **Batch/Reference Path:** **Azure Data Factory (ADF)** orchestrates metadata-driven pipelines to ingest static and slowly changing dimensional master datasets (e.g., driver profiles, zone mappings, payment gateways, vehicle tiers).

- **Declarative & Metadata-Driven Transformations:**
  Utilizes dynamic **Jinja2 templating** to parameterize Spark SQL and PySpark logic across notebooks, eliminating code duplication and enabling configuration-driven table generation.

- **Robust Medallion Data Lifecycle (Delta Lake):**
  - **Bronze Layer (Raw):** Ingests raw JSON events with append-only semantics, checkpointing, and strict schema retention to ensure full replayability.
  - **Silver Layer (Curated / Conformed):** Performs real-time schema enforcement, data deduplication, timezone standardizations, and joins streaming events with bulk mapping data into a consolidated One Big Table (OBT).
  - **Gold Layer (Analytical Model):** Structures curated data into a high-performance **Star Schema** featuring a centralized Fact table (`Fact_Trips`) and six Dimension tables (`Dim_Driver`, `Dim_Passenger`, `Dim_Location`, `Dim_Payment`, `Dim_Vehicle`, `Dim_Booking`), optimized for operational reporting and real-time KPI generation.

- **Production-Grade Design Patterns:**
  Implements watermarking, stream-stream/stream-static joins, idempotency via Delta Lake ACID transactions, and structural checkpointing to guarantee exactly-once processing semantics.
---


## 🏗️ Architecture

```
                                   ┌──────────────────────┐
                                   │   FastAPI Web App    │
                                   │   (Producer)         │
                                   │  "Book Ride" → Event │
                                   └──────────┬───────────┘
                                              │ Send Policy (auth)
                                              ▼
                                   ┌──────────────────────┐
                                   │  Azure Event Hub     │
                                   │  (Pub-Sub Channel)   │
                                   │  Partitions + Expiry │
                                   └──────────┬───────────┘
                                              │ Listen Policy (auth)
                                              ▼
                               ┌──────────────────────┐
      GitHub (Static/          │  Azure Databricks    │
      Mapping JSON files)      │  (Consumer)          │
            │                  └──────────┬───────────┘
            ▼                             │
   ┌──────────────────┐                   │
   │ Azure Data       │                   │
   │ Factory (ADF)    │                   │
   │ HTTP → Copy      │                   │
   │ Activity         │                   │
   └────────┬─────────┘                   │
            ▼                             │
   ┌──────────────────┐                   │
   │ ADLS Gen2        │◄──────────────────┘
   │ (Bronze Layer)   │   Raw streaming events landed
   └────────┬─────────┘
            ▼
   ┌──────────────────────────────────────────────────┐
   │              SILVER LAYER (Databricks)           │
   │  Initial_Load + Stream + 6x Mapping Joins (map)  │
   │  → One Big Table (OBT) / Streaming Table         │
   └────────────────────┬─────────────────────────────┘
                        ▼
   ┌────────────────────────────────────────────────────────┐
   │               GOLD LAYER (Databricks)                  │
   │  OBT → Join → JINJA-templated notebooks                │
   │                                                        │
   │   Fact Table:  Fact Booking                            │
   │   Dimensions:  Dim Location | Dim Booking              │
   │                Dim Vehicle  | Dim Passenger            │
   │                Dim Payment  | Dim Driver(SCD-enabled)  │
   └────────────────────────────────────────────────────────┘
```

### Data Flow Summary
1. **WebApp → Event Hub**: The FastAPI app acts as producer, generating simulated ride-booking events and pushing them to Event Hub.
2. **Event Hub → Databricks**: Databricks consumes the streaming events in real time via Spark Structured Streaming.
3. **GitHub → ADF → ADLS Gen2**: ADF pulls static/mapping bulk data (city, payment methods, vehicle types, initial historic load) from a GitHub repo into the Bronze layer of ADLS Gen2.
4. **Bronze → Silver**: Databricks joins streaming events with the mapping data to build a consolidated One Big Table (OBT).
5. **Silver → Gold**: The OBT is transformed into a star schema — 1 fact table + 6 dimension tables — with slowly changing dimension (SCD) support, using Jinja-templated, metadata-driven notebooks.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Event Ingestion | Azure Event Hub (managed Kafka-compatible service) |
| Event Producer | FastAPI + Python (Azure Event Hub SDK) |
| Batch/Static Data Orchestration | Azure Data Factory (ADF) |
| Data Lake Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Stream Processing & Transformation | Azure Databricks (Spark Structured Streaming, Declarative Pipelines) |
| Templating | Jinja2 |
| Data Modeling | Medallion Architecture (Bronze/Silver/Gold) + Star Schema with SCD |

---

## ✅ Prerequisites

- An **Azure Free/Paid account** with an active subscription
- Basic knowledge of **Python** and **SQL**
- Python 3.9+ installed locally (for the FastAPI producer app)
- Access to **Databricks** workspace (Premium tier recommended for Unity Catalog / streaming features)
- A GitHub account (to host the static/mapping JSON files, or fork the provided repo)
- Azure CLI / Azure Portal access

---

## 🚀 Project Setup & Implementation Steps

### Step 1 — Azure Event Hub Setup
1. In the Azure Portal, create an **Event Hubs Namespace**.
2. Inside the namespace, create an **Event Hub** (this is your ride-booking event stream).
3. Configure **retention/expiry** (1 hour – 7 days) based on how long events should be retained.
4. Set up **partitions** to enable parallel consumption.
5. Create two **Shared Access Policies**:
   - A **Send policy** → used by the producer (FastAPI app) to publish events.
   - A **Listen policy** → used by the consumer (Databricks) to read events.
6. Copy the connection strings for both policies — you'll need them in later steps.

### Step 2 — Simulated FastAPI Producer App
1. Clone/set up the provided FastAPI project locally.
2. Create a Python virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate   # (Windows: venv\Scripts\activate)
   pip install -r requirements.txt
   ```
3. Add your Event Hub **Send policy connection string** to the app's environment/config file (e.g. `.env`).
4. Run the FastAPI app:
   ```bash
   uvicorn main:app --reload
   ```
5. Open the web UI and click **"Book Ride"** — this triggers a function that generates pseudo-random ride-booking events (pickup/drop location, vehicle type, passenger, payment method, fare, etc.) and sends them asynchronously (in batches) to Event Hub via the Azure SDK for Python.
6. Verify events are arriving using Event Hub's **Data Explorer** in the Azure Portal.

### Step 3 — Bulk/Static Data on GitHub
1. Host static mapping JSON files in a GitHub repository — e.g. cities, payment methods, vehicle types.
2. Include an **initial bulk load** file with 2,000+ historic ride records to simulate a legacy data migration ("lift-and-shift").

### Step 4 — Azure Data Factory (ADF) Pipeline
1. Create an **ADF instance** in the Azure Portal.
2. Create **Linked Services**:
   - HTTP linked service → pointing to the GitHub raw file URLs.
   - ADLS Gen2 linked service → your target data lake.
3. Create **Datasets** for the source (GitHub JSON) and sink (ADLS Gen2).
4. Build a **Copy Activity** pipeline that pulls files from GitHub and lands them in ADLS Gen2 (Bronze layer).
5. Make the pipeline **metadata-driven** — parameterize source URLs/file names so new mapping files can be added via configuration, not code changes.
6. Trigger/run the pipeline and validate the files landed correctly in ADLS Gen2.

### Step 5 — Azure Databricks: Bronze → Silver
1. Provision an **Azure Databricks workspace** and cluster (or use serverless/DLT compute).
2. Mount/connect Databricks to your **ADLS Gen2** account and **Event Hub** namespace.
3. Set up a **Spark Structured Streaming** read from Event Hub (using the Listen policy connection string) to ingest ride-booking events continuously.
4. Read the mapping/static files from ADLS Gen2 (Bronze) as batch/reference data.
5. Using a **declarative pipeline** (Databricks' modern streaming pipeline framework), join the streaming events with the six mapping datasets (city, payment, vehicle, etc.) to enrich each record.
6. Land the enriched, consolidated result as a **Silver-layer streaming table** — a "One Big Table" (OBT) containing all enriched fields, so different domain teams can build off a single unified source.

### Step 6 — Azure Databricks: Silver → Gold (Star Schema)
1. From the Silver OBT, build the **Gold layer** star schema:
   - **1 Fact table**: `Fact_Booking` (ride transaction grain — booking ID, timestamps, fare, distance, foreign keys to dimensions)
   - **6 Dimension tables**: `Dim_Location`, `Dim_Booking`, `Dim_Vehicle`, `Dim_Passenger`, `Dim_Payment`, and one additional supporting dimension (per your data model).
2. Implement **Slowly Changing Dimensions (SCD)** logic (e.g. Type 1/Type 2) so dimension attributes update correctly over time as streaming data evolves.
3. Use **Jinja templates** to generate the notebook/SQL logic for each dimension and fact table dynamically from metadata (table name, keys, SCD type, columns) — this avoids hardcoding a separate notebook per table and makes onboarding new dimensions a config change.
4. Schedule/orchestrate the Silver→Gold notebooks (via Databricks Jobs or ADF) to run continuously or on a schedule.

### Step 7 — Validate End-to-End
1. Trigger new "Book Ride" events from the FastAPI app.
2. Confirm they flow through Event Hub → Databricks Silver (OBT) → Gold (Fact/Dim tables) in near real time.
3. Query the Gold-layer star schema in Databricks SQL to confirm joins, SCD behavior, and data correctness.

---

## 📂 Suggested Repository Structure

```
uber-streaming-project/
├── fastapi-app/                # Producer web app
│   ├── main.py
│   ├── event_generator.py
│   ├── requirements.txt
│   └── .env.example
├── adf-pipelines/               # ADF pipeline/dataset/linked service JSON definitions
├── databricks-notebooks/
│   ├── bronze/                  # Raw ingestion notebooks
│   ├── silver/                  # Streaming OBT + mapping joins
│   ├── gold/                    # Star schema (fact + dimensions), SCD logic
│   └── templates/               # Jinja templates for metadata-driven notebooks
├── mapping-data/                # Static JSON files (city, payment, vehicle, initial load)
└── README.md
```

---

## 🎯 Key Learning Outcomes

- Understand the **Pub-Sub / Producer-Consumer** model and how Azure Event Hub implements Kafka-compatible streaming.
- Build a real, working **real-time ingestion pipeline**: FastAPI → Event Hub → Databricks.
- Design **metadata-driven ADF pipelines** that scale without code changes.
- Apply **Spark Structured Streaming** with a modern declarative pipeline framework.
- Implement a **medallion architecture** (Bronze/Silver/Gold) combining streaming and batch data.
- Build a production-style **star schema** with SCD handling.
- Use **Jinja templating** for scalable, config-driven notebook generation.

---

## 📖 Recommended Next Steps

1. Set up your Azure free account and explore the portal.
2. Provision Event Hub namespaces, hubs, and Send/Listen policies.
3. Run the FastAPI app locally and confirm events land in Event Hub.
4. Build your first ADF pipeline (HTTP linked service + Copy Activity).
5. Set up Databricks and build the Silver streaming OBT.
6. Extend into the Gold star schema with SCD and Jinja templating.

---

## 📝 Credits

- Huge thanks to [Ansh Lamba](https://www.youtube.com/@AnshLambaJSR) for the incredible tutorial that walked me through this [project](https://www.youtube.com/watch?v=5KIbhHo6GJA) step by step 🙌.
