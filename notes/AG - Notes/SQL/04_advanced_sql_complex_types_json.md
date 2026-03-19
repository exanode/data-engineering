# 📘 DE Mentor Session: Advanced SQL (Part 4)
**Topic:** Semi-Structured Data (JSON, Arrays, Flattening)
**Goal:** Handle modern API/Kafka data payloads directly within SQL on Cloud Data Warehouses (15-18 LPA).

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** In the past, databases only accepted strict rows and columns (Relational). Today, Data Engineers ingest pure API payloads—massive JSON documents with nested arrays—directly into a single `Variant` or `JSON` column. Modern SQL dialects (Snowflake, BigQuery, Postgres) have functions to query, parse, and "flatten" these JSON objects into tabular rows.
*   **Why it exists:** Extract-Transform-Load (ETL) is dead. The modern paradigm is ELT (Extract, **Load**, Transform). We load raw JSON straight into the Data Warehouse so no data is ever lost. Then, we use Advanced SQL to unpack it.
*   **Where it's used:** Ingesting Shopify webhooks, mobile app telemetry, Kafka streams, and NoSQL (MongoDB) migrations.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: Extracting Values**
Using dot notation or operators (`->`, `:`) to pull a string out of a JSON blob.
*Example:* `SELECT raw_json:customer.name::string FROM events;`

**B. Intermediate: Unnesting Arrays (`FLATTEN` / `UNNEST`)**
Taking a JSON array `["apple", "banana"]` stored in one row, and exploding it into *two* separate rows.

**C. Advanced: Re-Aggregating (Rows to JSON)**
Taking standard relational rows and packing them back into a JSON array to send to a downstream API.
*Example:* `ARRAY_AGG(OBJECT_CONSTRUCT('id', user_id))`

---

## 3. Concrete SQL Examples (Step-by-Step)

### Example A: The FLATTEN Pattern (Snowflake syntax)
Imagine a table `raw_api_logs` with a JSON payload column containing an array of purchased items:
`{ "order_id": 1, "items": [{"name": "Laptop", "qty": 1}, {"name": "Mouse", "qty": 2}] }`

```sql
SELECT
    raw_payload:order_id::int as order_id,
    f.value:name::string as product_name,
    f.value:qty::int as quantity
FROM raw_api_logs,
LATERAL FLATTEN(input => raw_payload:items) f; 
-- LATERAL acts like a JOIN between the parent row and its own nested array!
```
*Output: 2 rows (one for Laptop, one for Mouse), both sharing the parent `order_id`.*

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "We receive a daily JSON file of user sessions where each session has an array of page views. How do you find the most viewed page?" (Answer: `LATERAL FLATTEN` the array into rows, then standard `GROUP BY`).
*   **Real-world DE Use Case:** A frontend team adds a new tracking parameter to their JSON logs. Because you use an ELT pattern with JSON columns, your pipeline doesn't break (Schema Evolution). You just update your downstream SQL view to extract the new key.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Failing to Cast:** JSON values are typically extracted as "Variant" types (meaning they still act like strings with quotes around them). You MUST explicitly cast them (`::string`, `::int`) or your subsequent `JOIN`s will silently fail or perform terribly.
2.  **Exploding Null Arrays:** If you `FLATTEN` an empty array without the `OUTER => TRUE` equivalent, the parent row disappears entirely (just like an `INNER JOIN`). Use Outer Flattening if you want to keep parent rows with empty item arrays.

---

## 6. Patterns & Mental Models
*   **The "Lateral Join" Mental Model:** Think of an Array inside a JSON row as its own miniature, hidden table. To read it, you must `JOIN` the parent row to this miniature table. That's why BigQuery uses `CROSS JOIN UNNEST(array)` and Postgres uses `LEFT JOIN LATERAL`.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"I enforce strict Third Normal Form (3NF). Array data belongs in a separate child table with a foreign key."*
*   **Data Engineer Mindset:** *"The application team iterates too fast for strict schemas. I will accept their raw JSON into a single column, guarantee the data lands, and enforce the schema horizontally using SQL views via `FLATTEN`."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** The specific unnesting function for your target dialect (e.g., Snowflake = `LATERAL FLATTEN`, BigQuery = `UNNEST()`).
*   **Understand:** Extracting JSON is relatively slow. In high-performance pipelines, extract the JSON *once* in a staging model, cast the fields to strict columnar data types, and let downstream users query the fast columnar data.

---

## 9. Practice Problems
1.  Given a JSON string `{"user": "Sachin", "skills": ["SQL", "Python"]}`, write pseudo-SQL to extract the second skill.
2.  Write a query to take a standard table of `Department` and `EmployeeName` and aggregate the names into a single JSON array per department.

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"We need to ingest NoSQL data with deeply nested arrays into our Data Warehouse. Should we write a Python script to parse it before loading?"*
**You:** > "No, writing custom Python parsers creates a brittle ETL pipeline. Instead, I would use an ELT approach. I'd use an ingestion tool like Fivetran or Snowpipe to dump the raw JSON directly into a `Variant` column in a landing table. From there, I'd use SQL native functions like `LATERAL FLATTEN` or `UNNEST` inside a dbt model to un-nest the arrays, explicitly cast the data types, and normalize it into dimensional tables. This guarantees we never drop data due to schema drift."
