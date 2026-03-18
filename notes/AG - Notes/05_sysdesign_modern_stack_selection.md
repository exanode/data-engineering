# 📘 DE Mentor Session: System Design (Part 5)
**Topic:** Modern Tech Stack Selection & Whiteboarding
**Reference:** *General Industry Standard (Medallion Architecture)*
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** The final stage of a System Design interview is taking all the concepts (Streaming, Storage Engines, Replication) and physically drawing the architecture out using named industry tools (Kafka, Snowflake, Python, dbt, Airflow).
*   **Why it exists:** Very rarely does a company ask you to build a database from scratch in C++. They ask you to *plumb* existing trillion-dollar software tools together gracefully to solve complex business ingestion needs.
*   **Where it's used:** The final round "Whiteboard" interview.

---

## 2. The Progression: The Modern Data Stack Blueprint

**A. Ingestion (The Front Door)**
*   **Batch Extractors (Fivetran / Airbyte):** Used when the source is an internal PostgreSQL database or Third-Party API (Salesforce) that we poll every night.
*   **Event Streams (Kafka / AWS Kinesis):** Used when the source is a high-volume live web-app (user clicks, IoT sensors).

**B. Storage (The Data Lake -> Warehouse)**
*   **Data Lake (AWS S3 / GCS):** The cheapest storage on earth. Everything (raw JSON, Parquet) lands here instantly.
*   **Data Warehouse (Snowflake / BigQuery):** The highly structured, columnar analytic engine that reads from the Lake.

**C. Transformation & Orchestration (The Brains)**
*   **Transformation (dbt):** Converts the ugly Bronze data in Snowflake into beautiful Gold Star-Schemas.
*   **Orchestration (Apache Airflow):** The alarm clock. It tells Fivetran to run at 1am, waits for it, tells dbt to run at 2am, waits for it, and emails the team if anything fails.

---

## 3. Concrete Architectural Examples (Step-by-Step)

### Interview Prompt: "Design the Data Infrastructure for Uber"
*Goal: We need live metrics on Driver locations for the mobile app, AND historical analytical reports for the Finance team on monthly revenue.*

**Step 1: Ingestion (Lambda Architecture)**
*   Millions of drivers send GPS coords every 5 seconds.
*   **Tool:** We pipe these mobile app API calls directly into **Apache Kafka**. (Why? Because an LSM-based append-only queue can absorb millions of rapid events without crashing).

**Step 2: The Speed Layer (Real-time App)**
*   **Tool:** We attach **Apache Flink** or **Spark Streaming** to Kafka.
*   It calculates driver ETA and surge pricing in memory, and sinks the result into **Redis** (an ultra-fast in-memory NoSQL database) so the mobile app can query it instantly.

**Step 3: The Batch Layer (Finance Reports)**
*   The raw Kafka events are also dumped simultaneously into **AWS S3** as highly compressed `.parquet` files.
*   **Snowflake** uses `Snowpipe` to continuous load these JSON files into a raw Variant landing table.

**Step 4: Transformation**
*   **dbt** executes incremental models via **Apache Airflow** on a nightly schedule, transforming the raw GPS data and transactional data into a dimensional Kimball Star Schema (Fact_Rides, Dim_Users).

**Step 5: Serving**
*   Finance points **Tableau/PowerBI** to the Gold layer dbt tables in Snowflake to generate the monthly revenue reports.

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** Interviewers want to see you intentionally choose tools based on their limits. "Why didn't you put the GPS data straight into PostgreSQL?" -> "Because Postgres uses a B-Tree storage engine which will heavily bottleneck on millions of rapid random writes causing lock contention. Kafka scales horizontally via partitioning to handle the throughput."
*   **Real-world DE Use Case:** This exact architecture is heavily used at companies like Netflix, Airbnb, and DoorDash!

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Ignoring Idempotency:** An Airflow DAG fails halfway through. You click "Retry" in the UI. Does your pipeline duplicate all the sales records from yesterday? **Gotcha fix:** Pipelines must be *Idempotent*. Running them 1 time or 100 times must yield the exact same end state in the database (e.g., heavily utilizing explicit `MERGE` logic or `DELETE FROM...` before inserting).
2.  **Forgetting Data Governance:** Interviewers love to trap you. "How do you handle GDPR 'Right to be forgotten' requests in this architecture?" (If a user's data is embedded inside immutable Parquet files in S3 and historical Snowflake Time Travel chunks... deleting them is actually incredibly hard!).

---

## 6. Patterns & Mental Models
*   **The Medallion Architecture (Databricks standard):**
    *   **Bronze:** Raw data. Exactly as it arrived from the source. Ugly JSON. Retained forever.
    *   **Silver:** Cleaned, filtered, decoded. Standardized datatypes. (dbt views/ephemerals).
    *   **Gold:** Business-level aggregates. Ready to be immediately consumed by the dumbest BI tools. (dbt incremental tables).

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"The application runs on APEX, the data sits in the Oracle DB underneath it, and we write reports using Oracle BI. It is a single, vendor-locked, monolithic stack."*
*   **Data Engineer Mindset:** *"The modern data stack is highly composable. I mix and match best-in-class decoupled modules. I use Fivetran for E, S3 for Storage, Snowflake for Compute, dbt for T, and Airflow for orchestration. I tie them together using APIs."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** Kafka/Kinesis = Queues. S3/GCS = Data Lake. Snowflake/BigQuery = Data Warehouse. dbt = Transformation. Airflow/Dagster = Orchestration. 
*   **Understand:** Be prepared to explicitly defend *why* you chose a specific tool. (e.g., "I chose Airflow over simple Cron jobs because Airflow provides native DAG dependency mapping and robust retry alerting protocols").

---

## 9. Practice Problems
1.  **System Design:** Grab a piece of paper. Draw boxes to architect a pipeline that collects web-scraping data of competitor pricing, cleans it, and triggers an email alert if a competitor drops a price below $10. Which tools do you use?
2.  **Trade-offs:** If a company does not have enough budget to afford Snowflake, what open-source alternatives could you deploy on AWS EC2 instances to mimic a Big Data warehouse? *(Hint: Look up Apache Hive, Presto/Trino, or ClickHouse).*

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"If we want to transition from our legacy on-premise transactional database into a Modern Data Stack capable of powering complex ML un-structured models and structured BI dashboards, architect the high-level flow."*
**You:** > "I would implement a standard ELT Medallion architecture. First, I would deploy an ingestion tool like Fivetran or build custom Airflow Python operators to pull batch data from our legacy DB, alongside tracking un-structured event telemetry via an Apache Kafka streaming queue. 
>
> All this raw data lands immediately into an AWS S3 Data Lake, forming our immutable Bronze layer, capable of supporting the ML team's unstructured notebook access. For the structured BI requirement, we utilize Snowflake's `COPY INTO` or Snowpipe features to pull that raw data into cloud compute. 
>
> Finally, I would deploy dbt, orchestrated via Apache Airflow, to enforce data quality tests and transform that raw schema into Silver and ultimately heavily aggregated Gold dimensional models. The BI visualization tools will strictly connect only to these highly-performant Gold tables, isolating analytical query load from our production transactional databases."
