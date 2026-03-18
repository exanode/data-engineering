# 📘 DE Mentor Session: Advanced SQL (Part 1)
**Topic:** Window Functions & Common Table Expressions (CTEs)
**Goal:** Shift mindset from Oracle APEX (OLTP) to Data Engineering (OLAP) — targeting the 15-18 LPA bracket.

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** In Oracle APEX, `GROUP BY` collapses multiple rows into a single summary row. **Window Functions** allow you to perform calculations across a "window" of related rows *without collapsing them*. You keep the original row-level details but attach aggregated or sequence data to them. **CTEs** (using the `WITH` clause) act as named, temporary result sets to make these complex queries readable.
*   **Why it exists:** DE isn't about fast, single-row transactions; it's about transforming millions of rows (ETL/ELT). You need to calculate running totals, find session times, and de-duplicate streaming events. Doing this with PL/SQL cursors or correlated subqueries on an OLAP engine (like Snowflake/Spark) will kill your pipeline's performance. Window functions scale across distributed nodes effortlessly.
*   **Where it's used:** Core to every Modern Data Stack. Used daily to clean Data Lake ingestion tables, build Slowly Changing Dimensions (SCDs), and guaranteed to be asked in *every* 15-18 LPA DE technical interview.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: Ranking Data (`ROW_NUMBER`, `RANK`, `DENSE_RANK`)**
Assigns sequential integers based on partitioned chunks.
*Use Case Example:* Deduplicating raw staging data.

**B. Intermediate: Lagging and Leading (`LAG`, `LEAD`)**
Grabs a value from the previous or next row in the partition.
*Use Case Example:* Tracking state changes, finding time elapsed between a user's web clicks.

**C. Advanced: Custom Window Frames (`ROWS BETWEEN`)**
Strictly limits the moving frame of rows (e.g., "From 6 rows ago to the current row").
*Use Case Example:* Calculating 7-day rolling averages or cumulative YTD sales.

---

## 3. Concrete SQL Examples (Step-by-Step)

### Example A: The Deduplication Pattern (Fundamentals)
Imagine an e-commerce stream (Kafka to Snowflake) where retries caused duplicate orders.
**Input Table:** `raw_orders` (order_id, status, updated_at)

```sql
WITH RankedOrders AS (
    SELECT
        order_id,
        status,
        updated_at,
        ROW_NUMBER() OVER(
            PARTITION BY order_id          -- 'Fence' the window per order
            ORDER BY updated_at DESC       -- 1 gets assigned to the LATEST true update
        ) as rn
    FROM raw_orders
)
SELECT order_id, status, updated_at
FROM RankedOrders
WHERE rn = 1; -- Beautiful, set-based deduplication!
```

### Example B: Time-over-Time Comparison (Intermediate)
Let's find out how much a person's salary changed from their *previous* title.
**Input Table:** `employee_history` (emp_id, salary, valid_from)

```sql
SELECT
    emp_id,
    salary,
    LAG(salary, 1) OVER(PARTITION BY emp_id ORDER BY valid_from) as prev_salary,
    -- Calculate the exact bump
    salary - LAG(salary, 1) OVER(PARTITION BY emp_id ORDER BY valid_from) as bump_amount
FROM employee_history;
```

---

## 4. Connection to Interviews & Real World

*   **Interview Connection:** "Find the top 3 highest-earning employees per department." At 15 LPA, they expect a CTE with `DENSE_RANK()`, not a bunch of clunky subqueries.
*   **Real-world DE Use Case:** **SCD Type 2**. In Data Warehousing, when a customer's address changes, you must close the old record (`effective_end_date`) and open a new one. `LEAD(start_date)` effortlessly pulls the next row's start date to use as the current row's end date.

---

## 5. Common Mistakes, Edge Cases, and Gotchas

1.  **The `WHERE` Clause Trap:** You **cannot** put a Window Function directly into a `WHERE` clause. It evaluates after `HAVING` but before `ORDER BY`. *Gotcha fix:* Always wrap it in a CTE first (like Example A).
2.  **`GROUP BY` vs. `PARTITION BY` Confusion:** Using `GROUP BY` collapses results. `PARTITION BY` just defines the boundaries of the window. Keep them mentally separate!
3.  **Default Frames Bug:** If you use an `ORDER BY` inside `OVER()` but don't specify the frame `ROWS BETWEEN`, the default behavior is **`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`**. This calculates a *running* total rather than a *partition-wide* total. Be explicit when doing aggregations.

---

## 6. Patterns & Mental Models

*   **The "Two-Step Filter" Pattern:** When an interviewer says "Find the Nth X", immediately write a CTE.
    *   *Step 1:* CTE with a Window Function (`rn = ROW_NUMBER()`).
    *   *Step 2:* Select from CTE `WHERE rn = N`.
*   **The "Gaps & Islands" Pattern:** Used to group consecutive sequences (like 3 consecutive login days). If you subtract `ROW_NUMBER()` from a strictly incrementing date, consecutive dates will yield the exact same "base" date (forming an island).

---

## 7. How to Think About It (The APEX to DE Shift)

*   **Oracle Dev Mindset:** *"I'll use a `FOR` loop and a `CURSOR` to check the previous row's value."*
*   **Data Engineer Mindset:** *"Cursors process row-by-row on a single node; they don't scale globally. Instead, I define a declarative **Frame of Reference**."*
*   Imagine a physical sliding cutout moving down an Excel spreadsheet:
    *   `PARTITION BY` = the solid walls your cutout cannot cross.
    *   `ORDER BY` = how the rows are sorted strictly inside those walls.

---

## 8. Summary: Memorize vs. Understand

*   **MUST Memorize (Syntax):**
    *   `SELECT function() OVER (PARTITION BY col1 ORDER BY col2) FROM table`
    *   The difference between `ROW_NUMBER`, `RANK`, and `DENSE_RANK`.
    *   The Two-Step Filter Pattern with CTEs.
*   **Should Understand (Concepts):**
    *   Why window functions scale horizontally on cluster compute (Spark/Snowflake).
    *   How SQL execution order dictates *why* you need CTEs to filter window results.

---

## 9. Practice Problems (Increasing Difficulty)

*Setup a mental schema or practice in PostgreSQL/Snowflake.*

1.  **Warm-up:** Write a query to assign a sequential ID (`1, 2, 3...`) to every login event, partitioned by `user_id` and ordered by `login_timestamp`.
2.  **Beginner:** Find the most recent purchase for each customer using `ROW_NUMBER` and a CTE.
3.  **Intermediate:** For a table of stock prices (`date`, `ticker`, `close`), calculate the 7-day moving average for each ticker. *(Hint: Use `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`)*
4.  **Advanced:** Given a `transactions` table, write a query to find the time difference (in days) between a user's current transaction and their previous one.
5.  **Expert (Interview Classic):** Write a query to find the top 3 highest-earning employees in every department. If there are ties for the 3rd spot, include *all* tied employees. *(Hint: `DENSE_RANK` is required over `ROW_NUMBER` here).*

---

## 10. The Interview-Quality Verbal Answer

**Interviewer:** *"Can you explain the difference between `RANK`, `DENSE_RANK`, and `ROW_NUMBER`? When would you use each?"*

**You (Verbally):**
> "All three are window functions used to sequence data within a partition, but they handle **ties** differently.
>
> `ROW_NUMBER` gives a strictly unique incrementing integer. Even if values are identical, it will arbitrarily assign 1 and 2. It’s my go-to for **de-duplicating** staging records where I strictly need one survivor.
>
> `RANK` and `DENSE_RANK` assign the same number to identical values. However, `RANK` skips the next number, leaving gaps (like 1, 2, 2, 4), whereas `DENSE_RANK` does not skip (1, 2, 2, 3).
>
> In an ETL pipeline, I use `DENSE_RANK` when stakeholders ask for 'Top N' reporting—like the top 3 selling products—ensuring we don't accidentally omit records if there's a tie for the top spots."

---
*Ready to review the practice problems or move to Part 2?*
