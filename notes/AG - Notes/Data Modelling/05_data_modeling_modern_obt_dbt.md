# 📘 DE Mentor Session: Data Modeling (Part 5)
**Topic:** Modern Analytics Engineering (dbt & OBT)
**Reference:** *Analytics Engineering with SQL and dbt* (From your roadmap)
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** Ralph Kimball wrote his books in the 1990s when disk space was incredibly expensive. To save space, he heavily emphasized strict Star Schemas (saving repetitive text strings into just the Dimensions). Today, with modern columnar architectures (Snowflake, BigQuery), Data Engineers are leaning into **OBT (One Big Table)** methodology and using tools like **dbt (Data Build Tool)** to construct pipelines purely via SQL `SELECT` statements.
*   **Why it exists:** Columnar databases excel when you don't use `JOIN`s. They scan single columns blindingly fast. If you take your Fact table and permanently `LEFT JOIN` all the Dimensions into it creating a monstrous 200-column table (OBT), Tableau can query it much faster than a standard Star Schema compute.
*   **Where it's used:** Core to the "Modern Data Stack." 15-18 LPA DE roles often border on "Analytics Engineering," meaning they expect you to know dbt intimately.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: The dbt Paradigm (ELT)**
Instead of writing Python scripts to `Extract` -> `Transform` -> `Load` (ETL), we load raw data straight into Snowflake. Then we write a `.sql` file in dbt with a simple `SELECT` statement. dbt automatically wraps it in a `CREATE TABLE AS` or `MERGE` and runs it on the warehouse.

**B. Intermediate: Layered Architecture (Medallion Architecture)**
*   **Bronze / Staging:** Lightly cleaned source data (renamed columns, CASTing JSON types).
*   **Silver / Intermediate:** Standard Kimball modeling (Facts and Dimensions, SCDs).
*   **Gold / Marts (OBT):** Taking the Silver Star Schema and flattening it out into highly denormalized One Big Tables for business units (e.g., `mart_marketing_dashboard`).

**C. Advanced: dbt Testing & Macros**
In APEX, you write unit tests for your PL/SQL packages. In dbt, you write YAML tests (`not_null`, `unique`, `accepted_values`) to automatically validate your data models before they are served to production.

---

## 3. Concrete SQL Examples (Step-by-Step)

### Building OBT using dbt references (`{{ ref() }}`)
In dbt, you don't hardcode table names. You use Jinja templating. This allows dbt to build a Dependency Graph (DAG) automatically.

**File:** `models/marts/marketing/obt_campaign_sales.sql`
```sql
WITH fact_sales AS (
    SELECT * FROM {{ ref('fact_store_sales') }}
),
dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),
dim_campaign AS (
    SELECT * FROM {{ ref('dim_campaign') }}
)

-- Flatten into One Big Table (OBT)
SELECT
    f.sales_amount,
    c.customer_name,
    c.customer_state,
    camp.campaign_name,
    camp.spend_amount
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_sk = c.customer_sk
LEFT JOIN dim_campaign camp ON f.campaign_sk = camp.campaign_sk;
```
*(When dbt runs this, it executes the joins and physically creates a new table `obt_campaign_sales` in Snowflake).*

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "What is dbt, and why would you use it over Airflow with scheduled Stored Procedures?" (Answer: dbt provides version-controllable SQL, automatic DAG inference, and built-in data quality testing out of the box).
*   **Real-world DE Use Case:** A company moves from Oracle to Snowflake. The Oracle DB had hundreds of complex PL/SQL materialized views. The DE migrates them by rewriting the logic as modular, DAG-based dbt models, massively simplifying the dependency hell.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **OBT Messiness:** The disadvantage of One Big Table is that it is hard to govern. If every analyst builds their own OBT, column definitions drift. OBTs should ONLY be built *downstream* of a strictly governed Kimball Star Schema (the Silver layer).
2.  **Skipping the Staging Layer:** Beginner DEs jump straight to building final Facts directly from the raw data. If the raw data schema changes, your complex Fact model breaks entirely. Always create a 1:1 Staging view with simple renames/casts as your foundation.

---

## 6. Patterns & Mental Models
*   **Treating Data as Code:** The biggest shift in modern DE. dbt forces you to treat Data Warehousing like Software Engineering. You use Git, CI/CD pipelines, modularity (DRY principles), and automated testing on your SQL. 

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"I write complex Stored Procedures containing DDL (`CREATE PROCEDURE`, `TRUNCATE TABLE`, `INSERT INTO`) to manage my transformations."*
*   **Data Engineer (dbt) Mindset:** *"Writing boilerplate DDL is a waste of time. I will write pure declarative business logic (`SELECT...`), and I rely on tool engines like dbt to figure out how to physically manifest that logically on the warehouse (as a View, Table, or Incremental Merge)."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** ELT separates extraction/loading from transformation. The Medallion Architecture (Bronze/Silver/Gold).
*   **Understand:** OBT (One Big Table) isn't a replacement for Kimball dimensional modeling; it's a modern optimization built *on top* of dimensional models to feed columnar BI tools.

---

## 9. Practice Problems
1.  **Architecture:** Explain the difference between ETL and ELT. Why does ELT make sense with Snowflake?
2.  **Tooling:** In an APEX environment, you validate data constraints on entry (using UI forms or DB constraints). In dbt, how and when do you validate data quality?

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"Many companies are dropping pure Kimball Star Schemas in favor of One Big Table (OBT). Do you think Star Schema is outdated? How do you architect data for Tableau?"*
**You:** > "Star Schema isn't outdated; it's just shifted layers. Directly feeding a massive Star Schema into Tableau often results in poor dashboard performance because Tableau has to execute complex remote joins. Modern columnar databases like BigQuery and Snowflake are astonishingly fast at scanning wide, flattened tables.
>
> Therefore, I use a layered architecture. I build a strict Kimball Star Schema in the intermediate analytical layer to ensure data governance, handle Slowly Changing Dimensions, and enforce conformed definitions. Then, as a final presentation layer using dbt, I orchestrate One Big Tables (OBTs). These are highly denormalized views built explicitly for Tableau, providing blazing-fast load times while maintaining the governed integrity of the underlying dimensional model."
