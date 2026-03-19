# 📘 DE Mentor Session: Data Modeling (Part 3)
**Topic:** Slowly Changing Dimensions (SCDs)
**Reference:** Highly tested in Interviews + *Fundamentals of Data Engineering*
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** In an APEX application, if a customer moves from New York to California, you just `UPDATE` their state column. But in a Data Warehouse, if you do that, *every single historical purchase* they made last year will suddenly look like it occurred in California! **Slowly Changing Dimensions (SCDs)** are the specific strategies we use to handle structural changes in our Dimension tables over time.
*   **Why it exists:** We are legally and financially obligated to report history as it actually was. "Truth" is tied to time.
*   **Where it's used:** Used on every single core Dimension table (`dim_customer`, `dim_product`) in the enterprise.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: SCD Type 1 (Overwrite)**
You just `UPDATE` the row. History is actively destroyed.
*Use Case:* Correcting a typo in a user's name ("Johnn" -> "John"). You don't care that they used to be misnamed.

**B. Intermediate: SCD Type 2 (Add New Row)**
The absolute gold standard. You don't overwrite. You add a **new row**, assign a new Surrogate Key, and use `valid_from` and `valid_to` date columns to track validity.
*Use Case:* Tracking User Address changes, Product Price changes.

**C. Advanced: SCD Type 3 (Add New Column)**
You keep the same single row, but you add an `old_value` and `current_value` column.
*Use Case:* A company-wide territory realignment where the sales team wants to see sales rolled up by *both* the old territory mapped structure and the new territory mapped structure simultaneously in the same dashboard.

---

## 3. Concrete SQL Examples (Step-by-Step)

### Visualizing SCD Type 2 Strategy (The Most Important)
**Day 1: Customer lives in NY.**
| customer_sk (PK) | natural_id | name | state | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|
| 101 | A-55 | Sachin | NY | 2023-01-01 | NULL | TRUE |

*(Sachin makes a purchase. The Fact table logs `customer_sk = 101`)*

**Day 2: Customer moves to CA. An ETL job runs a `MERGE` statement.**
| customer_sk (PK) | natural_id | name | state | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|
| 101 | A-55 | Sachin | NY | 2023-01-01 | **2023-08-05** | **FALSE** |
| **102** | A-55 | Sachin | CA | **2023-08-06** | NULL | **TRUE** |

*(Sachin makes a new purchase. The Fact table logs `customer_sk = 102`. The historical purchase remains tied to 101/NY!)*

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "We want to track our customer's subscription tier. How do you design the table?" (Answer: Immediately suggest SCD Type 2 with `valid_from`, `valid_to`, and `is_active` flags. No other answer is acceptable).
*   **Real-world DE Use Case:** Implementing this logic manually using SQL `MERGE` is tedious. Modern DEs use **dbt Snapshots**. dbt automatically writes the Type 2 logic for you—you just point it at a raw table, and it builds the `valid_to` columns dynamically based on `updated_at` timestamps!

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Forgetting to Update the Fact Table PIPELINE:** When a new Fact comes in, you must look up the Customer the moment the event happened. Your ETL pipeline must `JOIN` on `natural_id` AND ensure `Fact.event_date BETWEEN Dim.valid_from AND Dim.valid_to` to capture the correct Surrogate Key!
2.  **SCD Type 2 Explosion:** If a column updates every 5 minutes (like a "Last Login" timestamp), making it an SCD Type 2 will generate millions of rows and bloat the table exponentially. **Gotcha fix:** Move rapidly changing attributes out of the SCD dimension and into an entirely separate "Mini-Dimension" or put them in the Fact table.

---

## 6. Patterns & Mental Models
*   **The "Timestamp Fence" Model:** SCD Type 2 effectively fences reality using timestamps. If a business logic query ever misses the `WHERE is_current = TRUE` clause, it will double-count dimensions. Always provide analysts with a clean view referencing only the active rows for simple queries.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"To track changes, I'll create an `AUDIT_LOG_TABLE` triggered by an Oracle PL/SQL trigger."*
*   **Data Engineer Mindset:** *"Auditing is a backend compliance concern. Reporting on historical states is a BI concern. SCD Type 2 inherently builds the audit log into the business model itself, so the Tableau dashboard can seamlessly 'time travel' to any date in the past."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** Type 1 = Overwrite. Type 2 = Add Row + Date Columns. Type 3 = Add Column.
*   **Understand:** Type 2 requires assigning a totally new Surrogate Key every time a change happens. The Natural Key stays the same.

---

## 9. Practice Problems
1.  **Mental:** A supplier changes their address. The Data Science team says "We don't care about the history, just give us the current location." The Finance team says "We need it for historical auditing." What SCD Type do you use? (Hint: Type 2 satisfies both, just filter by `is_current`).
2.  **Verbal Check:** Why must you use a Surrogate Key instead of the original App ID when implementing SCD Type 2?

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"Explain what Slowly Changing Dimension Type 2 is, and how it impacts Fact table insertions."*
**You:** > "SCD Type 2 is a modeling technique used to track historical changes in dimension attributes. Instead of overwriting an updated value, we expire the existing record by setting a `valid_to` date, and we insert a completely new row with the updated value, new `valid_from` date, and crucially, a brand new Surrogate Key. 
>
> This heavily impacts how we load Fact tables. During our ETL pipeline, when an event arrives from the source system, we can't just join on the Natural ID. We must execute a point-in-time lookup: joining the Fact's event timestamp to the Dimension's `valid_from/valid_to` window. This ensures the Fact is stamped with the exact Surrogate Key representing the state of that dimension at the moment the event occurred."
