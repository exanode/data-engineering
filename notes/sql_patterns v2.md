# SQL Patterns — Complete Reference
## For: Sachin Ram | Goal: Data Engineer | Built for interviews + production thinking

---

# Part 1 — The Mental Foundation

---

## What "advanced SQL patterns" really means

These are not random tricks.

They exist because in real work and interviews, many questions are secretly one of these:

* "pick one row out of many"
* "compare this row to the previous row"
* "group events into runs or sessions"
* "find what is missing"
* "find change boundaries"
* "join safely without duplicating everything"
* "build business logic on ordered data"

So advanced SQL is mostly about learning to recognize the **shape of the problem**.

---

## The big mental shift

Beginner SQL thinks in terms of:

* `SELECT`
* `WHERE`
* `GROUP BY`
* `JOIN`

Advanced SQL thinks in terms of:

* **partition**
* **order**
* **row identity**
* **boundary detection**
* **group labeling**
* **dedup logic**
* **set difference**
* **safe joins**

---

## The 4-move master mental model

Most tricky SQL problems are solved in 4 moves.

**Move 1: define the grain**

What does one row represent?

* one order
* one user event
* one employee salary record
* one daily balance

If you get the grain wrong, everything breaks.

**Move 2: identify the grouping key**

What entity are we solving this for?

* per user
* per customer
* per product
* per department

**Move 3: identify ordering**

Does row sequence matter?

* by event_time
* by effective_date
* by transaction_id

**Move 4: choose the pattern**

Are we:

* ranking?
* deduping?
* detecting changes?
* finding streaks?
* sessionizing?
* anti-joining?
* comparing sets?

This is how senior people think.

---

## How SQL actually executes — why this matters

You write SQL top to bottom, but the database does **not execute it top to bottom**.

The practical execution order:

```
FROM / JOIN
WHERE
GROUP BY
HAVING
WINDOW FUNCTIONS
SELECT
DISTINCT
ORDER BY
LIMIT
```

**Key idea:** window functions are computed **after** `WHERE` and `GROUP BY`, but before final output.

That is why this fails:

```sql
SELECT
  customer_id,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
FROM orders
WHERE rn = 1;  -- rn does not exist at WHERE stage
```

The fix is a CTE or subquery:

```sql
WITH ranked AS (
  SELECT
    customer_id,
    order_id,
    order_time,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
  FROM orders
)
SELECT *
FROM ranked
WHERE rn = 1;
```

In Snowflake, `QUALIFY` solves this cleanly:

```sql
SELECT customer_id, order_id, order_time
FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) = 1;
```

`QUALIFY` filters after window functions without needing a subquery. This is Snowflake-specific — not portable to Postgres.

---

## Choosing between `DISTINCT`, `GROUP BY`, and window functions

This is a common interview differentiator because wrong SQL can still run but solve the wrong problem.

**The decision:**

* fewer rows → `GROUP BY`
* same rows + extra computed metrics → window functions
* just remove duplicate output rows, no business logic → `DISTINCT`

**Trap:** using `DISTINCT` after a bad join is not a fix. It hides a grain or cardinality bug.

**Trap:** using `GROUP BY` when you need row-level detail — you lose individual rows.

Mental model to say in interviews:

> "Should the result have fewer rows, the same rows, or just unique rows? That decides whether I use GROUP BY, a window, or DISTINCT."

---

## NULL semantics — the silent source of wrong SQL

NULL means "unknown" or "missing" — not zero, not empty string.

**`NULL = NULL` is not true. It is UNKNOWN.**

So `WHERE col = NULL` always fails. Use `WHERE col IS NULL`.

SQL uses three-valued logic: TRUE, FALSE, UNKNOWN. Rows pass `WHERE` only when condition is TRUE. UNKNOWN is filtered out.

**Practical example:**

```sql
SELECT * FROM users WHERE country <> 'IN';
```

If a row has `country = NULL`, the condition `NULL <> 'IN'` → UNKNOWN → row is filtered out silently.

**`COUNT(*)` vs `COUNT(col)`:**

* `COUNT(*)` counts rows
* `COUNT(col)` counts non-null values only

`SUM`, `AVG`, `MIN`, `MAX` all ignore NULLs.

**Null-safe comparison in Postgres:**

```sql
WHERE col1 IS DISTINCT FROM col2
```

This treats NULLs intuitively: NULL vs NULL → not distinct (equal), NULL vs 5 → distinct. Use this for change detection instead of `<>` wherever nullable columns exist.

---

## The 7-step mental checklist for any SQL problem

When given any advanced SQL problem, think in this order:

1. **Define the input grain** — what does one input row represent?
2. **Define the output grain** — what should one output row represent?
3. **Identify the entity key** — per user? per order? per department?
4. **Ask whether row order matters** — ranking, prior row, running logic, session boundaries?
5. **Estimate join cardinality** — will this join keep rows stable or multiply them?
6. **Think about NULL behavior** — could NULL break join, filter, comparison, anti-join, or count?
7. **Choose the right tool** — `GROUP BY`, window, anti-join, set difference, `MERGE`, incremental filter, or pre-aggregation before join

This is how strong candidates stay calm.

---

# Part 2 — The Core Patterns

---

## Pattern 1 — Latest row per key / deduplication

**Problem shape:**

"Give me the latest row per customer."
"Keep only the most recent status per order."
"Deduplicate event rows."

**Why it exists in DE work:**

* CDC streams produce multiple versions
* raw ingestion tables contain duplicates
* dimension updates arrive over time

**Postgres/standard SQL version:**

```sql
WITH ranked AS (
  SELECT
    customer_id,
    status,
    updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY updated_at DESC
    ) AS rn
  FROM customer_status
)
SELECT customer_id, status, updated_at
FROM ranked
WHERE rn = 1;
```

**Snowflake version with QUALIFY:**

```sql
SELECT customer_id, status, updated_at
FROM customer_status
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC
) = 1;
```

**Gotcha:** if two rows have the same `updated_at`, result is non-deterministic. Always use a tie-breaker:

```sql
ORDER BY updated_at DESC, load_id DESC
```

---

## Pattern 2 — Top N per group

**Problem shape:**

"Top 3 salaries per department."
"Most recent 2 orders per customer."

**Function choice:**

* `ROW_NUMBER()` → top N rows (deterministic, even on ties)
* `RANK()` → ties share rank, gaps exist after ties
* `DENSE_RANK()` → ties share rank, no gaps

Always clarify in interviews: do they want top 3 **rows** or top 3 **distinct values**? That decides the function.

```sql
WITH ranked AS (
  SELECT
    department,
    employee_id,
    salary,
    DENSE_RANK() OVER (
      PARTITION BY department
      ORDER BY salary DESC
    ) AS rnk
  FROM employees
)
SELECT *
FROM ranked
WHERE rnk <= 3;
```

---

## Pattern 3 — Change detection with `LAG`

**Problem shape:**

"Tell me when status changed."
"Find rows where price changed."
"Compare today to yesterday."

```sql
WITH x AS (
  SELECT
    order_id,
    status_time,
    status,
    LAG(status) OVER (
      PARTITION BY order_id
      ORDER BY status_time
    ) AS prev_status
  FROM order_status_history
)
SELECT
  *,
  CASE
    WHEN prev_status IS NULL THEN 1
    WHEN status IS DISTINCT FROM prev_status THEN 1
    ELSE 0
  END AS is_change
FROM x;
```

**How to think about it:** bring the previous row next to the current row, then compare.

**Gotcha:** use `IS DISTINCT FROM` instead of `<>` to handle NULLs correctly in nullable columns.

---

## Pattern 4 — Running totals and moving windows

**Problem shape:**

"Cumulative revenue."
"7-day moving average."
"Running balance."

**Running total:**

```sql
SELECT
  sale_date,
  amount,
  SUM(amount) OVER (
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM daily_sales;
```

**3-row moving average:**

```sql
SELECT
  sale_date,
  amount,
  AVG(amount) OVER (
    ORDER BY sale_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_3
FROM daily_sales;
```

**`ROWS` vs `RANGE`:**

* `ROWS` — counts physical rows. Deterministic.
* `RANGE` — groups rows by ordered value range. Tied values can be included together and surprise you.

Example with ties:

```
| day | amount |
|-----|--------|
|  1  |  100   |
|  1  |   50   |
|  2  |   30   |
```

With `ROWS`: first day-1 row gets 100, second day-1 row gets 150, day-2 gets 180.

With `RANGE`: both day-1 rows immediately see the full sum of all day=1 rows (150), because RANGE groups by the order key value.

**Practical rule:** default to `ROWS` for all operational and interview SQL. Only use `RANGE` when you explicitly want value-based grouping.

**Interview answer:**

> "I prefer ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW because it behaves on physical row positions. RANGE is value-based and pulls in tied rows sharing the same order key, so results differ when duplicates exist."

---

## Pattern 5 — Gaps and islands

**Problem shape:**

"Find consecutive login days."
"Find uninterrupted uptime streaks."
"Find consecutive trading days."

**The row_number subtraction trick:**

For consecutive dates, the difference `date - ROW_NUMBER()` stays constant across consecutive rows. That constant becomes the island label.

```sql
WITH x AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY login_date
    ) AS rn
  FROM logins
),
y AS (
  SELECT
    user_id,
    login_date,
    login_date - (rn * INTERVAL '1 day') AS grp
  FROM x
)
SELECT
  user_id,
  MIN(login_date) AS streak_start,
  MAX(login_date) AS streak_end,
  COUNT(*) AS streak_len
FROM y
GROUP BY user_id, grp
ORDER BY user_id, streak_start;
```

Memory aid: `grp = date - ROW_NUMBER() OVER (PARTITION BY user ORDER BY date)`

---

## Pattern 6 — Longest streak per group

Layer `ROW_NUMBER` on top of gaps and islands to find the longest one per user:

```sql
WITH x AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
  FROM logins
),
y AS (
  SELECT
    user_id,
    login_date,
    login_date - (rn * INTERVAL '1 day') AS grp
  FROM x
),
streaks AS (
  SELECT
    user_id,
    MIN(login_date) AS streak_start,
    MAX(login_date) AS streak_end,
    COUNT(*) AS streak_len
  FROM y
  GROUP BY user_id, grp
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY streak_len DESC, streak_end DESC
    ) AS rn
  FROM streaks
)
SELECT *
FROM ranked
WHERE rn = 1;
```

Patterns combined: row numbering → island labeling → aggregation → ranking again.

---

## Pattern 7 — Sessionization

**Problem shape:**

"Group user events into sessions where inactivity > 30 minutes."

Two steps: detect boundaries → cumulative-sum boundaries into group numbers.

```sql
WITH x AS (
  SELECT
    user_id,
    event_time,
    LAG(event_time) OVER (
      PARTITION BY user_id
      ORDER BY event_time
    ) AS prev_event_time
  FROM events
),
y AS (
  SELECT
    *,
    CASE
      WHEN prev_event_time IS NULL THEN 1
      WHEN event_time - prev_event_time > INTERVAL '30 minutes' THEN 1
      ELSE 0
    END AS is_session_start
  FROM x
)
SELECT
  *,
  SUM(is_session_start) OVER (
    PARTITION BY user_id
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS session_id
FROM y;
```

**Full session summary version:**

```sql
-- add one more CTE layer and aggregate:
SELECT
  user_id,
  session_id,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  COUNT(*) AS event_count
FROM (above query) z
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

---

## Pattern 8 — Anti-join

**Problem shape:**

"Find customers with no orders."
"Find source records not yet loaded."
"Find target rows no longer present in source."

**Three approaches:**

```sql
-- NOT EXISTS (safest, clearest intent)
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.customer_id
);

-- LEFT JOIN ... IS NULL (also valid)
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- NOT IN (DANGEROUS if subquery can contain NULLs)
-- If orders.customer_id has even one NULL, NOT IN returns no rows
-- because NULL <> anything is UNKNOWN, not FALSE.
-- Safe version only if you add: WHERE customer_id IS NOT NULL inside subquery.
```

**Which to prefer:**

* `NOT EXISTS` for clearest semantics, nullable-safe
* `LEFT JOIN ... IS NULL` when it fits a broader join pipeline naturally
* `NOT IN` only when you are certain the subquery column has no NULLs

**Verbal answer:**

> "I usually write this as NOT EXISTS because anti-join is really an existence check, and NOT EXISTS is robust even when nullable data is involved."

---

## Pattern 9 — Join explosion / fan-out detection

**Why it happens:** you assumed one-to-one but reality was one-to-many or many-to-many.

**Classic bug:**

```sql
-- orders has order_amount=500 for order 100
-- order_items has 2 rows for order 100 (item A and item B)
-- Naive join makes order_amount=500 appear twice
-- SUM(order_amount) returns 700+500 = 1200 instead of 700
SELECT SUM(o.order_amount)
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;
```

**Fix: pre-aggregate before joining**

```sql
WITH item_counts AS (
  SELECT order_id, COUNT(*) AS item_count
  FROM order_items
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.order_amount,
  i.item_count
FROM orders o
LEFT JOIN item_counts i ON o.order_id = i.order_id;
```

**Detection queries:**

```sql
-- check uniqueness of join key before joining
SELECT order_id, COUNT(*)
FROM order_items
GROUP BY order_id
HAVING COUNT(*) > 1;

-- compare row count before and after
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.order_id = oi.order_id;
```

Before every join, say this:

> "What is the grain on the left, what is the grain on the right, and what will one joined row represent?"

**Mistakes:**

* using `DISTINCT` after a bad join — hides the bug, does not fix it
* joining two fact-like tables directly without pre-aggregation
* not defining output grain first

---

## Pattern 10 — Set operations: `EXCEPT` / `INTERSECT`

**Problem shape:**

"What rows exist in source but not in target?"
"What rows changed between two snapshots?"

```sql
-- rows in source not in target (new or changed rows)
SELECT customer_id, city FROM source_snapshot
EXCEPT
SELECT customer_id, city FROM target_snapshot;

-- rows common to both
SELECT customer_id, city FROM source_snapshot
INTERSECT
SELECT customer_id, city FROM target_snapshot;
```

**Note:** `EXCEPT` compares full rows. If even one column differs, the row appears. This makes it powerful for change detection in SCD2.

---

## Pattern 11 — Correlated subqueries vs window functions

A correlated subquery re-executes for each outer row. It is not always wrong, but many analytics problems are cleaner with windows.

**Correlated subquery:**

```sql
SELECT e.emp_id, e.dept, e.salary
FROM employees e
WHERE e.salary > (
  SELECT AVG(e2.salary)
  FROM employees e2
  WHERE e2.dept = e.dept
);
```

**Window version (usually better for analytics):**

```sql
WITH x AS (
  SELECT
    emp_id, dept, salary,
    AVG(salary) OVER (PARTITION BY dept) AS dept_avg_salary
  FROM employees
)
SELECT emp_id, dept, salary
FROM x
WHERE salary > dept_avg_salary;
```

The window version computes the group average once and attaches it to every row, then filters. This is cleaner, more extensible, and better for layered analytics.

**If the problem says:** compare row to group metric, rank within group, or attach prior/next/group context — think window function first.

---

# Part 3 — SCD2: The Most Tested DE SQL Topic

---

## What SCD2 is

SCD2 = Slowly Changing Dimension Type 2. When a tracked attribute changes (e.g. customer city changes), you do not overwrite the old row. You close it and insert a new one. This preserves full history.

Every dimension table has:

| customer_key | customer_id | city   | valid_from | valid_to   | is_current |
|--------------|-------------|--------|------------|------------|------------|
| 10           | 1           | Delhi  | 2025-01-01 | 2025-03-14 | 0          |
| 11           | 1           | Mumbai | 2025-03-15 | 9999-12-31 | 1          |

Rules:
* `valid_to = 9999-12-31` and `is_current = 1` → active row
* when attribute changes: close old row, insert new row
* surrogate key (`customer_key`) is the DW identity — never the business key

---

## Step 1 — Detect changed rows

**Method A: join-based comparison**

```sql
SELECT
  s.customer_id,
  s.city AS new_city,
  d.city AS old_city
FROM source_customers s
JOIN dim_customers d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE s.city IS DISTINCT FROM d.city;
```

**Method B: EXCEPT (set-based)**

```sql
-- rows in source that differ from what is currently in dim
SELECT customer_id, city FROM source_customers
EXCEPT
SELECT customer_id, city FROM dim_customers WHERE is_current = 1;
```

**Method C: MD5 hashdiff (for multiple tracked columns)**

```sql
SELECT
  s.customer_id,
  MD5(CONCAT(s.city, '|', s.segment, '|', s.region)) AS src_hash,
  MD5(CONCAT(d.city, '|', d.segment, '|', d.region)) AS tgt_hash
FROM source_customers s
JOIN dim_customers d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE MD5(CONCAT(s.city, '|', s.segment, '|', s.region))
   <> MD5(CONCAT(d.city, '|', d.segment, '|', d.region));
```

Hashdiff is useful when you have many tracked columns — one comparison instead of N.

---

## Step 2 — Full SCD2 write pattern (Postgres)

This is the complete pattern interviewers expect you to write from scratch.

```sql
-- 1. Close old rows for changed records
UPDATE dim_customers
SET
  valid_to   = CURRENT_DATE - INTERVAL '1 day',
  is_current = 0
WHERE customer_id IN (
  -- changed records detected via join or EXCEPT
  SELECT s.customer_id
  FROM source_customers s
  JOIN dim_customers d
    ON s.customer_id = d.customer_id
   AND d.is_current = 1
  WHERE s.city IS DISTINCT FROM d.city
)
AND is_current = 1;

-- 2. Insert new rows for changed and brand-new records
INSERT INTO dim_customers (
  customer_id,
  city,
  valid_from,
  valid_to,
  is_current
)
SELECT
  s.customer_id,
  s.city,
  CURRENT_DATE         AS valid_from,
  '9999-12-31'::DATE   AS valid_to,
  1                    AS is_current
FROM source_customers s
LEFT JOIN dim_customers d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE d.customer_id IS NULL              -- brand new
   OR s.city IS DISTINCT FROM d.city;    -- changed
```

The two operations in plain English:
1. For changed records: mark the existing active row as closed.
2. For changed or new records: insert a fresh active row.

---

## Step 3 — SCD2 via MERGE (Snowflake)

Snowflake's MERGE is often the production way to write SCD2. But be aware: a single MERGE cannot both close old rows AND insert new ones in one shot cleanly. The standard Snowflake approach uses two passes, or a MERGE with a staging CTE trick:

```sql
-- Stage 1: close changed rows
MERGE INTO dim_customers tgt
USING (
  SELECT s.customer_id, s.city
  FROM source_customers s
  JOIN dim_customers d
    ON s.customer_id = d.customer_id
   AND d.is_current = 1
  WHERE s.city IS DISTINCT FROM d.city
) src
ON tgt.customer_id = src.customer_id AND tgt.is_current = 1
WHEN MATCHED THEN UPDATE SET
  tgt.valid_to   = CURRENT_DATE - 1,
  tgt.is_current = 0;

-- Stage 2: insert new active rows
MERGE INTO dim_customers tgt
USING source_customers src
ON tgt.customer_id = src.customer_id AND tgt.is_current = 1
WHEN NOT MATCHED THEN INSERT (customer_id, city, valid_from, valid_to, is_current)
  VALUES (src.customer_id, src.city, CURRENT_DATE, '9999-12-31', 1)
WHEN MATCHED AND src.city IS DISTINCT FROM tgt.city THEN INSERT
  VALUES (src.customer_id, src.city, CURRENT_DATE, '9999-12-31', 1);
```

**Note on Snowflake MERGE:** Snowflake supports `WHEN NOT MATCHED BY SOURCE` which Postgres does not. This lets you handle deletes or close rows where source record disappeared.

---

## Step 4 — Late-arriving records

**Problem:** a record arrives 3 days after its actual effective date. The current active row was already closed.

**What to do:**

1. Find which row's `valid_from` / `valid_to` window the late record falls into
2. Close the current row at the late-arriving event date
3. Insert the late-arriving version with correct `valid_from` = event date
4. Reopen the prior-current version if needed

**Simplified late-arrival insert:**

```sql
-- Assume late record arrived with effective_date = '2025-03-01'
-- but we are processing it on '2025-03-10'
-- and the dim currently shows a row that was valid from '2025-02-01'

-- Step 1: close the row that should have been closed on 2025-03-01
UPDATE dim_customers
SET valid_to = '2025-03-01'::DATE - INTERVAL '1 day',
    is_current = 0
WHERE customer_id = :customer_id
  AND is_current = 1
  AND valid_from < '2025-03-01';

-- Step 2: insert the late-arriving version
INSERT INTO dim_customers (customer_id, city, valid_from, valid_to, is_current)
VALUES (:customer_id, :new_city, '2025-03-01', '9999-12-31', 1);
```

**Verbal answer for interviews:**

> "Late-arriving records require me to find the right place in the history timeline, split the existing row at the correct effective date, insert the late version, and then decide whether the post-late state needs a new row too. In practice I use a lookback window in my incremental pipeline so that records arriving up to N days late still get picked up, and my MERGE handles idempotent upsert logic."

**Trade-offs to mention:**

* lookback window (simple, safe, small cost increase)
* full reprocessing of affected partition (correct but expensive)
* event-time vs ingestion-time watermarking (architectural choice)

---

## SCD2 via dbt snapshot — the production bridge

In real DE work with a modern stack, SCD2 on dimensions is usually implemented via **dbt snapshots** rather than raw SQL.

```yaml
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}
  {{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
  }}
  SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```

dbt generates `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` automatically.

**Interview framing:**

> "I'd implement SCD2 via a dbt snapshot in the warehouse layer — it generates valid_from / valid_to automatically and handles the close-and-insert logic. If I were outside dbt, I'd write raw SQL using UPDATE to close changed rows followed by INSERT for new versions, using IS DISTINCT FROM for null-safe column comparison."

Being able to say both — raw SQL AND dbt path — is what separates strong candidates at 15–18 LPA.

---

# Part 4 — MERGE: Full Pattern Reference

---

## What MERGE does

MERGE combines INSERT, UPDATE, and DELETE into one statement based on a join condition.

It is the standard tool for:
* SCD2 implementation
* upsert patterns
* incremental pipeline loads
* CDC-style target table maintenance

---

## Core MERGE syntax (Snowflake)

```sql
MERGE INTO target_table tgt
USING source_table src
ON tgt.id = src.id

WHEN MATCHED AND tgt.hash_val <> src.hash_val THEN
  UPDATE SET
    tgt.col1 = src.col1,
    tgt.col2 = src.col2,
    tgt.updated_at = src.updated_at

WHEN NOT MATCHED THEN
  INSERT (id, col1, col2, updated_at)
  VALUES (src.id, src.col1, src.col2, src.updated_at)

WHEN NOT MATCHED BY SOURCE THEN
  DELETE;  -- Snowflake-specific: rows in target with no source match
```

---

## Core MERGE syntax (Postgres — 15+)

```sql
MERGE INTO target_table tgt
USING source_table src
ON tgt.id = src.id

WHEN MATCHED THEN
  UPDATE SET
    col1 = src.col1,
    col2 = src.col2

WHEN NOT MATCHED THEN
  INSERT (id, col1, col2)
  VALUES (src.id, src.col1, src.col2);
```

Postgres MERGE does not support `WHEN NOT MATCHED BY SOURCE` — that is Snowflake-only.

---

## Idempotent MERGE

Running the same MERGE twice should produce the same result.

**Why it matters:** pipelines fail and rerun. If MERGE is not idempotent, you get duplicates or corruption.

**How to ensure it:**

1. Use a business key as the join condition (`ON tgt.id = src.id`)
2. Only update when data actually changed (hash comparison or column comparison)
3. Never INSERT the same key twice — the `WHEN MATCHED` clause handles existing rows

```sql
-- Idempotent upsert: run this twice for the same source data
-- and the target row count stays identical
MERGE INTO orders_target tgt
USING orders_source src
ON tgt.order_id = src.order_id
WHEN MATCHED AND tgt.updated_at < src.updated_at THEN
  UPDATE SET
    tgt.status = src.status,
    tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, status, updated_at)
  VALUES (src.order_id, src.status, src.updated_at);
```

Test: run this twice on the same source. Row count in target must be identical.

---

## MERGE with hashdiff (cleaner for many columns)

```sql
MERGE INTO dim_customers tgt
USING (
  SELECT
    customer_id,
    city,
    segment,
    MD5(CONCAT_WS('|', city, segment)) AS row_hash
  FROM source_customers
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
  UPDATE SET
    tgt.city     = src.city,
    tgt.segment  = src.segment,
    tgt.row_hash = src.row_hash
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, segment, row_hash)
  VALUES (src.customer_id, src.city, src.segment, src.row_hash);
```

---

## MERGE strategy: merge vs delete+insert (Snowflake / dbt)

In dbt incremental models on Snowflake:

* `incremental_strategy: merge` — uses MERGE to upsert. Updates existing rows, inserts new. Requires `unique_key`.
* `incremental_strategy: delete+insert` — deletes matching rows in target, then bulk inserts. Simpler but causes micro-partition churn if over-used.

**When to use each:**

* Use `merge` when rows update frequently and you want precise row-level upserts
* Use `delete+insert` when you are replacing entire date partitions and want clean partition replacement

---

# Part 5 — Incremental Processing, Watermarking, and Idempotency

---

## Why incremental processing exists

If a table has billions of rows, you do not want to recompute everything every day. So you process only new or changed data.

This reduces runtime, cost, and failure surface.

---

## Three core strategies

**1. Full refresh**

Rebuild the entire target every run.

Good when: data is small, logic is simple, correctness matters most, early model stage.

Bad when: data is huge or SLA is tight.

**2. Incremental append**

Load only newly arrived rows.

```sql
SELECT *
FROM source_events
WHERE event_time > (
  SELECT MAX(event_time)
  FROM target_events
);
```

Simple but fragile — misses late-arriving data.

**3. Incremental upsert / MERGE**

Insert new rows and update changed rows. See MERGE section above. This is the production standard for dimensions and latest-state tables.

---

## Watermarking

A watermark is the "how far have I processed?" marker.

```sql
SELECT *
FROM source_orders
WHERE updated_at > :last_watermark;
```

**Why watermarks are tricky:**

Suppose yesterday you processed up to `2025-01-10 10:00:00`. A late record arrives with `updated_at = 2025-01-10 09:55:00`. Strict `>` watermark misses it.

**Fix: lookback window**

```sql
SELECT *
FROM source_orders
WHERE updated_at >= :last_watermark - INTERVAL '1 day';
```

Then deduplicate downstream using MERGE or unique_key.

---

## Idempotency

> Running the same pipeline again should not create incorrect duplicates or inconsistent results.

**Non-idempotent load:**

```sql
INSERT INTO target_orders
SELECT * FROM source_orders
WHERE order_date = CURRENT_DATE;
```

If the job reruns, same rows get inserted again.

**Idempotent patterns:**

* MERGE with business key
* DELETE WHERE dt = X + INSERT (partition replacement)
* dbt incremental model with `unique_key`

**The DELETE + INSERT pattern (simple and clean):**

```sql
DELETE FROM target_orders WHERE order_date = :run_date;

INSERT INTO target_orders
SELECT * FROM source_orders
WHERE order_date = :run_date;
```

Run this twice for the same date: identical result every time.

---

## Late-arriving data

Common in real systems: mobile devices sync late, APIs retry late, upstream batches are delayed.

**Handling approaches:**

* lookback window in watermark query (pick up records up to N days late)
* event-time vs ingestion-time logic (process by when it happened, not when it arrived)
* dedup by business key + latest timestamp downstream
* periodic backfill for data older than lookback window

**Common interview questions:**

* full refresh vs incremental: when would you choose each?
* how do you handle late-arriving data?
* how do you make an incremental load idempotent?
* why is watermark-only loading risky?

**Mental model for recurring SQL pipelines:**

1. what identifies a row uniquely?
2. what marks new or changed data?
3. can data arrive late?
4. if the job reruns, will I duplicate data?
5. when should I rebuild fully instead?

That is data engineering thinking.

---

# Part 6 — Cohort Analysis

---

## What cohort analysis is

Group users by when they first did something (first purchase, first login, first activation), then track their behaviour over time relative to that starting point.

Common interview question. Appears in product analytics and DE take-home problems.

---

## Standard cohort retention query

**Problem:** given `orders(user_id, order_date)`, compute how many users from each signup month made a purchase in subsequent months.

```sql
WITH first_order AS (
  -- each user's cohort = month of their very first order
  SELECT
    user_id,
    DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders
  GROUP BY user_id
),
user_activity AS (
  -- join back to get all orders with cohort label attached
  SELECT
    o.user_id,
    f.cohort_month,
    DATE_TRUNC('month', o.order_date) AS activity_month
  FROM orders o
  JOIN first_order f ON o.user_id = f.user_id
),
cohort_data AS (
  -- period index: how many months after cohort month is this activity?
  SELECT
    cohort_month,
    activity_month,
    -- months between cohort start and activity month
    EXTRACT(YEAR FROM AGE(activity_month, cohort_month)) * 12
    + EXTRACT(MONTH FROM AGE(activity_month, cohort_month)) AS period_number,
    COUNT(DISTINCT user_id) AS active_users
  FROM user_activity
  GROUP BY cohort_month, activity_month
)
SELECT
  cohort_month,
  period_number,
  active_users,
  FIRST_VALUE(active_users) OVER (
    PARTITION BY cohort_month
    ORDER BY period_number
  ) AS cohort_size,
  ROUND(
    100.0 * active_users /
    FIRST_VALUE(active_users) OVER (
      PARTITION BY cohort_month
      ORDER BY period_number
    ), 1
  ) AS retention_pct
FROM cohort_data
ORDER BY cohort_month, period_number;
```

**What the output looks like:**

| cohort_month | period_number | active_users | cohort_size | retention_pct |
|--------------|---------------|--------------|-------------|---------------|
| 2025-01-01   | 0             | 500          | 500         | 100.0         |
| 2025-01-01   | 1             | 310          | 500         | 62.0          |
| 2025-01-01   | 2             | 220          | 500         | 44.0          |
| 2025-02-01   | 0             | 450          | 450         | 100.0         |
| 2025-02-01   | 1             | 270          | 450         | 60.0          |

**How to think about it:**

* period 0 = same month as cohort → always 100% retention
* period 1 = one month later → what % came back?
* `FIRST_VALUE` window function picks the cohort size (period 0 count) for the denominator

---

# Part 7 — Partition Pruning and Warehouse-Aware SQL

---

## The shift from OLTP to warehouse thinking

In OLTP: "use an index."

In cloud warehouses: prune partitions / micro-partitions, scan fewer files, align filters with storage layout.

---

## Partition pruning

Partition pruning means the engine skips irrelevant data chunks.

**Good — engine reads only January partitions:**

```sql
SELECT *
FROM sales
WHERE order_date >= DATE '2025-01-01'
  AND order_date < DATE '2025-02-01';
```

**Bad — engine must inspect all partitions:**

```sql
SELECT *
FROM sales
WHERE YEAR(order_date) = 2025;  -- function on column kills pruning
```

Wrapping the partition column in a function breaks pruning. Always filter on the raw column with a range.

---

## Clustering / sort keys

Different warehouses, same core idea — organize physical storage so common filters and joins scan less data:

* Snowflake: clustering keys
* Redshift: sort keys
* BigQuery: clustering + partitioning

If your largest table is frequently filtered by `event_date` and `customer_id`, those are strong candidates for partition/clustering design.

---

## Strong query optimization answer

If asked "how would you optimize a slow query on a huge fact table?":

* filter early — push WHERE conditions as far in as possible
* project only needed columns — columnar systems charge you for columns scanned
* avoid functions on filter columns — kills pruning
* align filters with partition keys
* consider clustering/sort design for common predicates
* pre-aggregate before joins to prevent fan-out

> "Can the engine skip most of the data, or am I forcing it to scan everything?"

---

# Part 8 — Multi-Pattern Examples (How Hard Problems Are Built)

---

The nastiest SQL questions combine 2–3 patterns. Recognizing that is the skill.

---

## Full example 1 — Longest consecutive login streak per user

Patterns: dedup → row numbering → gaps/islands → aggregation → ranking

```sql
WITH deduped AS (
  SELECT DISTINCT user_id, login_date
  FROM user_logins
),
numbered AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
  FROM deduped
),
islands AS (
  SELECT
    user_id,
    login_date,
    login_date - (rn * INTERVAL '1 day') AS grp
  FROM numbered
),
streaks AS (
  SELECT
    user_id,
    MIN(login_date) AS streak_start,
    MAX(login_date) AS streak_end,
    COUNT(*) AS streak_len
  FROM islands
  GROUP BY user_id, grp
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY streak_len DESC, streak_end DESC
    ) AS rn
  FROM streaks
)
SELECT user_id, streak_start, streak_end, streak_len
FROM ranked
WHERE rn = 1
ORDER BY user_id;
```

---

## Full example 2 — Session summaries

Patterns: LAG → boundary detection → cumulative sum → aggregation

```sql
WITH x AS (
  SELECT
    user_id,
    event_time,
    event_name,
    LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
  FROM events
),
y AS (
  SELECT
    *,
    CASE
      WHEN prev_event_time IS NULL THEN 1
      WHEN event_time - prev_event_time > INTERVAL '30 minutes' THEN 1
      ELSE 0
    END AS is_session_start
  FROM x
),
z AS (
  SELECT
    *,
    SUM(is_session_start) OVER (
      PARTITION BY user_id
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM y
)
SELECT
  user_id,
  session_id,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  COUNT(*) AS event_count
FROM z
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

---

## Pattern combination reference

| Problem | Patterns needed |
|---------|----------------|
| Longest paid subscription streak per user | gaps & islands + filtering + ranking |
| Latest active plan excluding canceled | filter + dedup latest-row + anti-join |
| Count sessions per user per day | sessionization + date extraction + aggregation |
| Find products never sold after launch | anti-join + time filter |
| Top 3 longest inactivity gaps per customer | LAG + interval calc + ranking |
| Cohort retention | first event date + period index + window ratio |

---

# Part 9 — Common Mistakes and Gotchas

---

1. **Not defining the grain** — if one row means "order item" but you think it means "order," your query lies.

2. **Missing deterministic ordering** — window functions need stable ordering. Add a tie-breaker: `ORDER BY updated_at DESC, load_id DESC`

3. **Filtering too early** — filtering rows before `LAG`, `LEAD`, ranking, or sessionization destroys the sequence.

4. **Wrong ranking function** — `ROW_NUMBER` vs `RANK` vs `DENSE_RANK` is not cosmetic. Know when ties matter.

5. **Ignoring null semantics** — `NULL <> NULL` is not true. Use `IS DISTINCT FROM` for nullable columns in change detection.

6. **Using LEFT JOIN without thinking about multiplicity** — metrics inflate silently.

7. **DISTINCT to hide a bad join** — this is hiding a bug, not fixing it.

8. **Using `RANGE` accidentally** — can behave unexpectedly on tied order values. Default to `ROWS`.

9. **`NOT IN` with nullable subquery** — can return no rows. Use `NOT EXISTS` or add `WHERE col IS NOT NULL` inside subquery.

10. **`= NULL` in WHERE** — always wrong. Use `IS NULL`.

---

# Part 10 — What to Memorize vs What to Understand

---

## Must memorize (say/write from memory)

* window execution cannot be filtered in `WHERE` — use CTE or `QUALIFY`
* `QUALIFY` filters after window functions (Snowflake/BigQuery, not Postgres)
* `COUNT(*)` counts rows, `COUNT(col)` counts non-null values
* `NULL = NULL` is not true — use `IS NULL`
* `NOT EXISTS` is safer than `NOT IN` when NULLs may exist
* `ROWS` is row-position-based, `RANGE` is value-based — default to `ROWS`
* before joins, define grain and cardinality
* incremental pipelines need idempotency
* functions on partition/filter columns hurt pruning
* SCD2 = close old row (set valid_to, is_current=0) + insert new row
* MERGE is idempotent only if you have a business key and check before updating
* late-arriving data needs lookback window + downstream dedup

---

## Must understand deeply (concept, not rote syntax)

* why SQL execution order causes window filtering issues
* why `DISTINCT` is not a fix for a bad join
* how fan-out inflates metrics
* why NULL creates UNKNOWN, not FALSE
* why late-arriving data breaks naive watermark logic
* why warehouse performance depends on both query and storage layout
* why window functions are often better than correlated subqueries for analytics
* why SCD2 needs both a close step and an insert step
* why idempotency matters when pipelines fail and rerun

---

# Part 11 — Interview Verbal Answers

---

## General advanced SQL

> "For advanced SQL problems, I first define the grain of the input and the required grain of the output, because most mistakes come from grain mismatch or join fan-out. Then I check whether the problem is aggregation, row preservation with window functions, or a set/existence problem like an anti-join. I'm also careful about NULL semantics, because NOT IN, joins, and comparisons can silently behave differently when NULLs are present. For ranking and latest-row problems, I usually use ROW_NUMBER, then filter either with a CTE or QUALIFY in Snowflake. In production data engineering, I also think about incremental loading, idempotency, partition pruning, and whether my joins will multiply rows or scan too much data."

## Pattern identification

> "Advanced SQL problems usually reduce to a few reusable patterns. I first identify the grain of the data, the grouping key, and whether row order matters. Then I map the problem to a pattern such as deduplication with ROW_NUMBER, change detection with LAG, sessionization with LAG plus cumulative SUM, gaps-and-islands with a row-number grouping trick, or anti-joins with NOT EXISTS. In real data engineering work, these patterns show up in CDC deduping, session analytics, incremental loads, reconciliation, and SCD2 history tracking. My approach is to solve them in stages with CTEs so the logic stays correct and explainable."

## SCD2

> "I'd implement SCD2 via a dbt snapshot in the warehouse layer — it generates valid_from and valid_to automatically and handles the close-and-insert logic. If I were outside dbt, I'd write raw SQL using UPDATE to close changed rows followed by INSERT for new versions, using IS DISTINCT FROM for null-safe column comparison. For late-arriving records, I use a lookback window in my incremental pipeline so records arriving up to a few days late still get captured."

## ROWS vs RANGE

> "I prefer ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW because it behaves on physical row positions. RANGE is value-based and can pull in tied rows that share the same order key, so results differ when duplicates exist in the ordering column."

## Anti-join

> "I usually write this as NOT EXISTS because anti-join is really an existence check, and NOT EXISTS is robust even when nullable data is involved. LEFT JOIN IS NULL is also valid, but I avoid NOT IN unless I know the subquery cannot return NULLs."

---

# Part 12 — Practice Problems

---

## 1. Easy

Given `employees(emp_id, dept, salary)`, return each employee row along with department average salary. Use a window function, not GROUP BY.

## 2. Easy-Medium

Why does this query fail, and how do you fix it?

```sql
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
FROM orders
WHERE rn = 1;
```

Write both: portable fix (CTE) and Snowflake fix with QUALIFY.

## 3. Medium

Given `customers(customer_id)` and `orders(order_id, customer_id)`, return customers with no orders using NOT EXISTS and LEFT JOIN IS NULL. Explain which you prefer and why.

## 4. Medium

Show with a small example why NOT IN can return wrong results when the subquery contains NULL. Then rewrite it safely.

## 5. Medium-Hard

Given `sales(day, amount)` with duplicate `day` values, demonstrate the difference between ROWS and RANGE behavior. Explain the tie behavior.

## 6. Hard

Given `orders(order_id, customer_id, order_amount)` and `order_items(order_id, item_id)`, show how a naive join inflates total order revenue. Then fix it.

## 7. Hard

Rewrite this correlated subquery using a window function and explain why the window version is better:

```sql
SELECT e.*
FROM employees e
WHERE salary > (
  SELECT AVG(salary)
  FROM employees e2
  WHERE e2.dept = e.dept
);
```

## 8. Hard

Design an incremental load for `source_orders(order_id, updated_at, amount)` into `target_orders`. Explain: full refresh vs incremental, watermark, idempotency, late-arriving data handling.

## 9. Hard

Write the full SCD2 SQL for `dim_customers(customer_id, city)` from scratch: detect changes, close old rows, insert new rows. No reference.

## 10. Hard

A query on a huge `events` table filtered by `event_date` is slow:

```sql
SELECT *
FROM events
WHERE YEAR(event_date) = 2025;
```

Explain why this hurts partition pruning and rewrite it properly.

## 11. Hard

Given `source_snapshot` and `dim_customers` (is_current = 1), detect changed rows using EXCEPT. Then extend to MD5 hashdiff for a 3-column comparison.

## 12. Hard

Write an idempotent MERGE for a `target_orders` table. Run the same MERGE twice for the same source data. Confirm result is identical.

---

# Final Note

Advanced SQL is not about memorizing 200 tricks.

It is about mastering a small set of reusable ideas:

* **rank rows**
* **compare adjacent rows**
* **accumulate over ordered rows**
* **label boundaries**
* **group consecutive runs**
* **exclude matches**
* **compare sets**
* **control join cardinality**
* **preserve history across changes**
* **run pipelines safely, repeatedly, correctly**

Once you can recognize those shapes and connect them to production DE reality — CDC deduping, SCD2, incremental pipelines, idempotency, warehouse performance — "tricky" SQL stops feeling magical.
