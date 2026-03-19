# SQL Patterns — Complete Reference
### Sachin Ram | Oracle/PL/SQL → Data Engineer 15–18 LPA
### Use for: learning, DataLemur, StrataScratch, NamasteSql, LeetCode, light book reference

---

# How this file is organised

**Part 1 — The Mental Foundation**
How to think about SQL problems before writing a single line.

**Part 2 — SQL Execution Order and Why It Matters**
Why certain SQL fails, what QUALIFY solves, and how to think in stages.

**Part 3 — NULL Semantics**
The silent source of wrong SQL. Read this before any pattern.

**Part 4 — Core Patterns (1–11)**
Every pattern you need, with input tables, full queries, annotated output, how to think about it, and common mistakes.

**Part 5 — Choosing the Right Tool**
DISTINCT vs GROUP BY vs window functions. Correlated subqueries vs windows.

**Part 6 — Join Cardinality and Fan-Out**
The most important real-world DE skill that interviewers test indirectly.

**Part 7 — Window Frames Deep Dive**
ROWS vs RANGE, defaults, and why ties surprise you.

**Part 8 — SCD2: Detection, Full Write, Late Data, dbt**
The most tested DE topic at 15–18 LPA. Full SQL from scratch.

**Part 9 — MERGE: Full Reference**
Postgres vs Snowflake, idempotency, hashdiff, 10 scenarios, dbt strategy choice.

**Part 10 — Incremental Processing, Watermarking, Idempotency**
Where SQL becomes data engineering.

**Part 11 — Cohort Analysis**
Standard product analytics pattern. Know it cold.

**Part 12 — Partition Pruning and Warehouse-Aware SQL**
Why the same query can be 100x faster or slower depending on how you write it.

**Part 13 — Pattern Combinations and Multi-Pattern Examples**
How hard problems are built. Full worked examples.

**Part 14 — How to Attack SQL Problems in Interviews**
The 5-step process. Pattern reference table. Real work DE usage.

**Part 15 — What to Memorise vs What to Understand**
Muscle-memory snippets. Concept-level understanding list.

**Part 16 — Interview Verbal Answers**
Polished answers you can actually say.

**Part 17 — Practice Problems**
Ordered by difficulty. Covers all patterns.

**Part 18 — Study Schedule and Final Checklist**

---

# Part 1 — The Mental Foundation

---

## What "advanced SQL patterns" really means

These are not random tricks.

They exist because in real work and interviews, most hard questions are secretly one of these:

* "pick one row out of many"
* "compare this row to the previous row"
* "group events into runs or sessions"
* "find what is missing"
* "find change boundaries"
* "join safely without duplicating everything"
* "build business logic on ordered data"

Advanced SQL is mostly about learning to recognise the **shape of the problem**.

---

## The big mental shift

Beginner SQL thinks in terms of:

* `SELECT` · `WHERE` · `GROUP BY` · `JOIN`

Advanced SQL thinks in terms of:

* **partition** · **order** · **row identity** · **boundary detection**
* **group labelling** · **dedup logic** · **set difference** · **safe joins**

---

## The 4-move master mental model

Most tricky SQL problems are solved in 4 moves.

**Move 1 — Define the grain**

What does one row represent?

* one order · one user event · one employee salary record · one daily balance

If you get the grain wrong, everything breaks.

**Move 2 — Identify the grouping key**

What entity are we solving this for?

* per user · per customer · per product · per department

**Move 3 — Identify ordering**

Does row sequence matter?

* by event_time · by effective_date · by transaction_id

**Move 4 — Choose the pattern**

Are we ranking? Deduping? Detecting changes? Finding streaks? Sessionising?
Anti-joining? Comparing sets? Managing history?

This is how senior people think.

---

## The 11 core patterns

1. Latest row per key / dedup
2. Top N per group
3. Change detection with LAG / LEAD
4. Running totals and moving windows
5. Gaps and islands
6. Longest streak per group
7. Sessionisation
8. Anti-join
9. Join explosion / fan-out detection
10. Set operations — EXCEPT / INTERSECT
11. SCD2 change detection and full write

Your roadmap explicitly calls out every single one of these. They are the non-negotiable SQL core for 15–18 LPA.

---

# Part 2 — SQL Execution Order and Why It Matters

---

## Intuition

You write SQL top to bottom. The database does **not execute it top to bottom**.

This matters because many interview traps are really execution-order traps:

* "Why can't I use `ROW_NUMBER()` in `WHERE`?"
* "Why do I need a subquery or CTE?"
* "Why does `QUALIFY` help?"

---

## The practical execution order

```
FROM / JOIN       ← tables are assembled here
WHERE             ← rows are filtered here (window aliases don't exist yet)
GROUP BY          ← rows are grouped here
HAVING            ← grouped rows are filtered here
WINDOW FUNCTIONS  ← window calculations happen here
SELECT            ← column expressions are evaluated here
DISTINCT          ← duplicate output rows removed
ORDER BY          ← result is sorted
LIMIT / FETCH     ← result is truncated
```

**The key insight:** window functions run *after* WHERE and GROUP BY, but *before* ORDER BY and output.

That is why this fails:

```sql
-- BROKEN: rn does not exist at the WHERE stage
SELECT
  customer_id,
  order_id,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
FROM orders
WHERE rn = 1;
-- ERROR: column "rn" does not exist
```

---

## The fix: layered CTEs

Break the problem into stages. Compute first, filter after.

```sql
WITH ranked AS (
  SELECT
    customer_id,
    order_id,
    order_time,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY order_time DESC
    ) AS rn
  FROM orders
)
SELECT customer_id, order_id, order_time
FROM ranked
WHERE rn = 1;
```

Think of it as:

1. CTE: build rows and compute window values
2. Outer query: filter on those values

This "layered CTE thinking" is one of the most important habits in advanced SQL.

---

## QUALIFY — the Snowflake shortcut

`QUALIFY` filters *after* window functions without needing a subquery. It is the `HAVING` of window functions.

```sql
-- Snowflake / BigQuery only — not portable to Postgres
SELECT customer_id, order_id, order_time
FROM orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY order_time DESC
) = 1;
```

Where `QUALIFY` is used:

* latest row per key
* top N per group
* dedup
* first event after a condition
* any time you need to filter on a window result

**Gotcha:** `QUALIFY` is not supported in Postgres. In interviews, always know both versions — CTE/subquery for portability, QUALIFY for Snowflake.

---

## Why GROUP BY aliases also fail in WHERE

The same execution-order logic explains why this fails:

```sql
-- BROKEN
SELECT sale_date, SUM(amount) AS total
FROM daily_sales
WHERE total > 100;  -- total doesn't exist at WHERE stage
```

Fix:

```sql
SELECT sale_date, SUM(amount) AS total
FROM daily_sales
GROUP BY sale_date
HAVING SUM(amount) > 100;  -- HAVING runs after GROUP BY
```

`HAVING` is to `GROUP BY` what `QUALIFY` is to window functions.

---

# Part 3 — NULL Semantics: The Silent Source of Wrong SQL

---

## Intuition

NULL does not mean zero. NULL does not mean empty string.

NULL means: **unknown** or **missing**.

Because of that, SQL comparisons involving NULL behave differently than most people expect. Many "mystery bugs" are really NULL bugs.

---

## The first rule: `NULL = NULL` is not true

It is UNKNOWN. So this is always wrong:

```sql
WHERE col = NULL    -- never matches anything
```

Correct:

```sql
WHERE col IS NULL
WHERE col IS NOT NULL
```

---

## Three-valued logic

SQL uses TRUE, FALSE, UNKNOWN.

Rows pass a `WHERE` filter only when the condition is TRUE.
UNKNOWN means the row is **silently filtered out**.

```sql
-- Table: users
-- | user_id | country |
-- |---------|---------|
-- |    1    |   IN    |
-- |    2    |  NULL   |
-- |    3    |   US    |

SELECT * FROM users WHERE country <> 'IN';
-- Returns only row 3.
-- Row 2: NULL <> 'IN' → UNKNOWN → filtered out.
-- Many people expect rows 2 and 3. They get only row 3.
```

---

## COUNT(*) vs COUNT(col)

```sql
-- Table: payments
-- | payment_id | amount |
-- |------------|--------|
-- |     1      |  100   |
-- |     2      |  NULL  |
-- |     3      |   50   |

SELECT
  COUNT(*)      AS total_rows,       -- 3
  COUNT(amount) AS non_null_amounts  -- 2
FROM payments;
```

`COUNT(*)` counts rows. `COUNT(col)` counts non-null values in that column.

SUM, AVG, MIN, MAX all **ignore NULLs** automatically.

---

## IS DISTINCT FROM — null-safe comparison

In Postgres:

```sql
WHERE col1 IS DISTINCT FROM col2
```

This treats NULLs intuitively:

* NULL vs NULL → not distinct (they are "equal")
* NULL vs 5   → distinct
* 5 vs 5      → not distinct

Use this instead of `<>` wherever nullable columns appear in change detection, SCD2, and LAG comparisons.

In Snowflake, the equivalent is `col1 IS DISTINCT FROM col2` (same syntax, supported).

---

## NULLs inside joins

Left joins produce NULLs on the right side when there is no match. This is the foundation of the anti-join pattern (Part 4, Pattern 8).

NULLs in join keys can cause rows to be silently excluded — two NULLs do not match each other in a JOIN condition.

---

## NULLs and NOT IN — the most dangerous trap

```sql
-- If orders.customer_id contains even one NULL, this returns NO rows at all:
SELECT customer_id FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
```

Why: SQL evaluates `customer_id NOT IN (1, 2, NULL)`. For any value, one of the comparisons becomes `value <> NULL` → UNKNOWN. The overall NOT IN becomes UNKNOWN. Row is filtered out.

Safe version:

```sql
WHERE customer_id NOT IN (
  SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
)
```

Or better, use `NOT EXISTS` — see Pattern 8.

---

# Part 4 — Core Patterns

---

## Pattern 1 — Latest row per key / deduplication

### Problem shape

"Give me the latest row per customer."
"Keep only the most recent status per order."
"Deduplicate event rows — keep the newest version."

### Why it exists in DE work

* CDC streams produce multiple versions of the same record
* raw ingestion tables contain duplicates from retries
* dimension tables receive updates over time
* bronze → silver cleanup

### Input

```
customer_status
| customer_id | status | updated_at       |
|-------------|--------|------------------|
|      1      | bronze | 2025-01-01 10:00 |
|      1      | silver | 2025-01-10 09:00 |
|      1      | gold   | 2025-01-15 12:00 |
|      2      | bronze | 2025-01-03 08:00 |
|      2      | silver | 2025-01-04 08:00 |
```

### Query — Postgres / standard SQL

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

### Query — Snowflake (QUALIFY)

```sql
SELECT customer_id, status, updated_at
FROM customer_status
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY updated_at DESC
) = 1;
```

### Output

```
| customer_id | status | updated_at       |
|-------------|--------|------------------|
|      1      |  gold  | 2025-01-15 12:00 |
|      2      | silver | 2025-01-04 08:00 |
```

### How to think about it

For each customer: sort rows newest → oldest, assign row numbers, keep first row.

### Gotcha — non-deterministic ties

If two rows have the same `updated_at`, result may be non-deterministic. Always use a tie-breaker:

```sql
ORDER BY updated_at DESC, load_id DESC
```

---

## Pattern 2 — Top N per group

### Problem shape

"Top 3 salaries per department."
"Most recent 2 orders per customer."
"Top 5 products by revenue per category."

### The three ranking functions

| Function | Tie behaviour | When to use |
|---|---|---|
| `ROW_NUMBER()` | Ties get different ranks. Deterministic. | When you want exactly N rows |
| `RANK()` | Ties share rank. Gaps after ties. | When ties are meaningful |
| `DENSE_RANK()` | Ties share rank. No gaps. | When you want top N distinct values |

**Always clarify in interviews:** do they want top 3 **rows** or top 3 **salary levels**? That decides the function.

### Input

```
employees
| department | employee_id | salary |
|------------|-------------|--------|
|    Eng     |     1       |  90000 |
|    Eng     |     2       |  90000 |
|    Eng     |     3       |  80000 |
|    Eng     |     4       |  70000 |
|   Sales    |     5       |  60000 |
|   Sales    |     6       |  55000 |
```

### Query — top 3 distinct salary levels per department

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
SELECT department, employee_id, salary, rnk
FROM ranked
WHERE rnk <= 3;
```

### Output

```
| department | employee_id | salary | rnk |
|------------|-------------|--------|-----|
|    Eng     |      1      |  90000 |  1  |
|    Eng     |      2      |  90000 |  1  |  ← both get rank 1
|    Eng     |      3      |  80000 |  2  |
|    Eng     |      4      |  70000 |  3  |
|   Sales    |      5      |  60000 |  1  |
|   Sales    |      6      |  55000 |  2  |
```

With `ROW_NUMBER` instead of `DENSE_RANK`, only one of emp_id 1 and 2 would be returned (arbitrary without secondary sort). Know which you need.

---

## Pattern 3 — Change detection with LAG / LEAD

### Problem shape

"Tell me when status changed."
"Find rows where price changed from the previous day."
"Mark first event after inactivity."
"Compare today to yesterday."

### Intuition

`LAG(col)` brings the *previous* row's value next to the current row.
`LEAD(col)` brings the *next* row's value next to the current row.
Then you compare.

### Input

```
order_status_history
| order_id | status_time | status  |
|----------|-------------|---------|
|   101    |    09:00    | placed  |
|   101    |    09:05    | placed  |
|   101    |    09:10    | packed  |
|   101    |    09:20    | shipped |
```

### Query

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
  order_id,
  status_time,
  status,
  prev_status,
  CASE
    WHEN prev_status IS NULL              THEN 1  -- first row for this order
    WHEN status IS DISTINCT FROM prev_status THEN 1  -- status changed
    ELSE 0
  END AS is_change
FROM x;
```

### Output

```
| order_id | status_time | status  | prev_status | is_change |
|----------|-------------|---------|-------------|-----------|
|   101    |    09:00    | placed  |    NULL     |     1     |
|   101    |    09:05    | placed  |   placed    |     0     |
|   101    |    09:10    | packed  |   placed    |     1     |
|   101    |    09:20    | shipped |   packed    |     1     |
```

### Why IS DISTINCT FROM instead of `<>`

If `status` can be NULL, then `NULL <> 'placed'` → UNKNOWN → is_change gets 0 when it should be 1.

`IS DISTINCT FROM` handles NULLs correctly: NULL vs 'placed' → they are distinct → is_change = 1.

### LAG with offset and default

```sql
-- Look back 2 rows, default to 0 if no prior row
LAG(amount, 2, 0) OVER (PARTITION BY user_id ORDER BY event_date)
```

### LEAD — looking forward

```sql
-- How long until the next event?
LEAD(event_time) OVER (PARTITION BY user_id ORDER BY event_time) - event_time AS time_to_next
```

---

## Pattern 4 — Running totals and moving windows

### Problem shape

"Cumulative revenue."
"7-day moving average."
"Running balance per account."
"30-day rolling sum."

### Syntax skeleton

```sql
aggregate_function(col) OVER (
  [PARTITION BY key]
  ORDER BY ordering_col
  ROWS BETWEEN start AND end
)
```

### Frame boundaries

| Keyword | Meaning |
|---|---|
| `UNBOUNDED PRECEDING` | First row of the partition |
| `N PRECEDING` | N rows before current row |
| `CURRENT ROW` | Current row |
| `N FOLLOWING` | N rows after current row |
| `UNBOUNDED FOLLOWING` | Last row of the partition |

### Input

```
daily_sales
| sale_date  | amount |
|------------|--------|
| 2025-01-01 |  100   |
| 2025-01-02 |   80   |
| 2025-01-03 |   50   |
| 2025-01-04 |  120   |
| 2025-01-05 |   60   |
```

### Query — running total

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

```
| sale_date  | amount | running_total |
|------------|--------|---------------|
| 2025-01-01 |  100   |     100       |
| 2025-01-02 |   80   |     180       |
| 2025-01-03 |   50   |     230       |
| 2025-01-04 |  120   |     350       |
| 2025-01-05 |   60   |     410       |
```

### Query — 3-day moving average

```sql
SELECT
  sale_date,
  amount,
  ROUND(AVG(amount) OVER (
    ORDER BY sale_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS moving_avg_3d
FROM daily_sales;
```

```
| sale_date  | amount | moving_avg_3d |
|------------|--------|---------------|
| 2025-01-01 |  100   |    100.00     |  ← only 1 row available
| 2025-01-02 |   80   |     90.00     |  ← 2 rows: (100+80)/2
| 2025-01-03 |   50   |     76.67     |  ← 3 rows: (100+80+50)/3
| 2025-01-04 |  120   |     83.33     |  ← (80+50+120)/3
| 2025-01-05 |   60   |     76.67     |  ← (50+120+60)/3
```

### Query — 7-day moving average (roadmap asks for this specifically)

```sql
AVG(amount) OVER (
  ORDER BY sale_date
  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

### Running total partitioned by user

```sql
SELECT
  user_id,
  event_date,
  purchase_amount,
  SUM(purchase_amount) OVER (
    PARTITION BY user_id
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_spend
FROM purchases;
```

---

## Pattern 5 — ROWS vs RANGE (window frames deep dive)

This deserves its own section because interviewers ask about it directly, and the default behaviour surprises people.

### The difference

**`ROWS`** — counts physical rows. Fully deterministic.

**`RANGE`** — groups rows by *value* of the ORDER BY column. Tied values are treated as a group.

### Why the default matters

If you write `SUM(amount) OVER (ORDER BY sale_date)` without specifying a frame, many engines apply a default equivalent to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

This means: for tied `sale_date` values, the running total already includes *all rows with the same date* on the very first row of that date.

### Example with ties

```
sales
| day | amount |
|-----|--------|
|  1  |  100   |
|  1  |   50   |
|  2  |   30   |
```

```sql
-- ROWS version
SUM(amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- day=1 row 1:  100       (only this row)
-- day=1 row 2:  150       (100 + 50)
-- day=2 row 1:  180       (100 + 50 + 30)

-- RANGE version
SUM(amount) OVER (ORDER BY day RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- day=1 row 1:  150       (BOTH day=1 rows included — same value)
-- day=1 row 2:  150       (same)
-- day=2 row 1:  180
```

The RANGE version jumps straight to 150 on the first day=1 row because it groups all rows with day=1 together.

### Practical rule

**Default to `ROWS` for all operational and interview SQL.**

Use `RANGE` only when you explicitly want value-based grouping behaviour (rare).

### Interview answer

> "I prefer ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW because it behaves on physical row positions. RANGE is value-based and pulls in tied rows that share the same order key, so the running total can jump unexpectedly when duplicates exist."

---

## Pattern 6 — Gaps and islands

### Problem shape

"Find consecutive login days."
"Find uninterrupted uptime streaks."
"Find consecutive stock trading days."
"Find attendance streaks."

### Intuition

A **gap** is a missing period. An **island** is a consecutive run.

The trick: for consecutive dates, the difference `date − ROW_NUMBER()` stays constant. That constant becomes the island label.

### Input

```
logins
| user_id | login_date |
|---------|------------|
|    1    | 2025-01-01 |
|    1    | 2025-01-02 |
|    1    | 2025-01-03 |  ← island 1 ends here (gap follows)
|    1    | 2025-01-05 |
|    1    | 2025-01-06 |  ← island 2
|    2    | 2025-01-10 |
|    2    | 2025-01-12 |  ← gap between 10 and 12
```

### Why the subtraction trick works

```
user 1:
| login_date | rn | date - rn * 1 day        |
|------------|----|--------------------------| 
| 2025-01-01 |  1 | 2025-01-01 - 1 = Dec 31  | ← grp A
| 2025-01-02 |  2 | 2025-01-02 - 2 = Dec 31  | ← grp A  (same constant)
| 2025-01-03 |  3 | 2025-01-03 - 3 = Dec 31  | ← grp A
| 2025-01-05 |  4 | 2025-01-05 - 4 = Jan 01  | ← grp B  (constant shifted)
| 2025-01-06 |  5 | 2025-01-06 - 5 = Jan 01  | ← grp B
```

### Query — all islands

```sql
WITH numbered AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY login_date
    ) AS rn
  FROM logins
),
islands AS (
  SELECT
    user_id,
    login_date,
    login_date - (rn * INTERVAL '1 day') AS grp
  FROM numbered
)
SELECT
  user_id,
  MIN(login_date) AS streak_start,
  MAX(login_date) AS streak_end,
  COUNT(*)        AS streak_len
FROM islands
GROUP BY user_id, grp
ORDER BY user_id, streak_start;
```

### Output

```
| user_id | streak_start | streak_end | streak_len |
|---------|--------------|------------|------------|
|    1    | 2025-01-01   | 2025-01-03 |     3      |
|    1    | 2025-01-05   | 2025-01-06 |     2      |
|    2    | 2025-01-10   | 2025-01-10 |     1      |
|    2    | 2025-01-12   | 2025-01-12 |     1      |
```

### Memory aid

`grp = date − ROW_NUMBER() OVER (PARTITION BY user ORDER BY date)`

---

## Pattern 7 — Longest streak per group

Layer `ROW_NUMBER` on top of the islands to find only the longest streak per user.

```sql
WITH numbered AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) AS rn
  FROM (SELECT DISTINCT user_id, login_date FROM logins) deduped
  -- deduplicate first in case same user logged in twice on the same day
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
    COUNT(*)        AS streak_len
  FROM islands
  GROUP BY user_id, grp
),
ranked_streaks AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY streak_len DESC, streak_end DESC  -- longest first; tie-break by most recent
    ) AS rn
  FROM streaks
)
SELECT user_id, streak_start, streak_end, streak_len
FROM ranked_streaks
WHERE rn = 1
ORDER BY user_id;
```

Patterns combined: dedup → row numbering → island labelling → aggregation → ranking. This is advanced SQL.

---

## Pattern 8 — Sessionisation

### Problem shape

"Group user events into sessions when inactivity > 30 minutes."
"Assign a session ID to each event."
"Compute session duration and event count per session."

### Intuition

Two steps:
1. Detect session boundaries (where a gap in time > threshold occurs)
2. Cumulative-sum the boundary flags → each new boundary starts a new session number

### Input

```
events
| user_id | event_time |
|---------|------------|
|    1    |   10:00    |
|    1    |   10:05    |
|    1    |   10:50    |  ← gap > 30 min: new session
|    1    |   11:00    |
|    1    |   12:10    |  ← gap > 30 min: new session
```

### Query — assign session IDs

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
  user_id,
  event_time,
  is_session_start,
  SUM(is_session_start) OVER (
    PARTITION BY user_id
    ORDER BY event_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS session_id
FROM y;
```

### Output

```
| user_id | event_time | is_session_start | session_id |
|---------|------------|------------------|------------|
|    1    |   10:00    |        1         |     1      |
|    1    |   10:05    |        0         |     1      |
|    1    |   10:50    |        1         |     2      |
|    1    |   11:00    |        0         |     2      |
|    1    |   12:10    |        1         |     3      |
```

### Query — full session summaries

```sql
-- add one more layer after session_id assignment
SELECT
  user_id,
  session_id,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  COUNT(*)        AS event_count,
  MAX(event_time) - MIN(event_time) AS session_duration
FROM (
  -- paste the session_id query above here as a CTE or subquery
  ...
) sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

---

## Pattern 9 — Anti-join

### Problem shape

"Find customers with no orders."
"Find source records not yet loaded into the warehouse."
"Find employees not assigned to a department."
"Find target rows no longer present in source."

### Three approaches

**Pattern A — NOT EXISTS (safest, clearest intent)**

```sql
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.customer_id
);
```

For each customer: "does at least one matching order exist? If no, keep the customer."

**Pattern B — LEFT JOIN IS NULL**

```sql
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

Left join keeps all customers. Where no match exists, order columns are NULL. Filter for NULL → unmatched customers only.

**Pattern C — NOT IN (dangerous when NULLs present)**

```sql
-- DANGEROUS if orders.customer_id can contain NULLs
SELECT customer_id FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
```

If the subquery returns even one NULL, `NOT IN` returns **no rows at all**. See Part 3 for why.

Safe version:

```sql
WHERE customer_id NOT IN (
  SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
)
```

### Which to use

* Default to `NOT EXISTS` — it expresses intent clearly and handles NULLs correctly
* `LEFT JOIN IS NULL` is also valid, especially when it fits naturally in a broader join chain
* Avoid `NOT IN` unless you are certain the subquery cannot return NULLs

### Input / output example

```
customers: 1, 2, 3, 4
orders: customer_id in (1, 1, 2)

NOT EXISTS result: 3, 4
```

### Interview verbal answer

> "I usually write anti-joins as NOT EXISTS because it expresses intent clearly — I am asking whether a matching row exists, not joining data together. It also handles NULLs correctly unlike NOT IN."

---

## Pattern 10 — Join explosion / fan-out detection

### Problem shape

"You joined two tables and row counts exploded."
"Revenue doubled after a join."
"My metric is inflated — why?"

### Why it happens

You assumed one-to-one. Reality was one-to-many or many-to-many.

Every join has a cardinality contract:

| Join type | Row count after |
|---|---|
| one-to-one | Same as left |
| one-to-many | Expands — each left row multiplied by matching right rows |
| many-to-one | Same as left |
| many-to-many | Can explode — every left row × every matching right row |

### Classic analytics bug

```
orders
| order_id | customer_id | order_amount |
|----------|-------------|--------------|
|   100    |      1      |     500      |
|   101    |      1      |     200      |

order_items
| order_id | item_id | item_price |
|----------|---------|------------|
|   100    |    A    |    300     |
|   100    |    B    |    200     |
|   101    |    C    |    200     |
```

```sql
-- Naive join — looks fine but is wrong
SELECT SUM(o.order_amount) AS total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;
```

```
-- order 100 joins to 2 items → order_amount 500 appears TWICE
-- Result: 500 + 500 + 200 = 1200
-- Correct answer: 500 + 200 = 700
-- Revenue inflated by 71%
```

### Detection before joining

```sql
-- Check if join key is unique on right side before joining
SELECT order_id, COUNT(*)
FROM order_items
GROUP BY order_id
HAVING COUNT(*) > 1;
-- If this returns rows, a direct join will expand your left table
```

```sql
-- Compare row counts before and after join
SELECT COUNT(*) FROM orders;                                         -- 2
SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.order_id = oi.order_id; -- 3
-- Row count went up → grain changed → be careful with aggregations
```

### Safe pattern: pre-aggregate before joining

```sql
WITH item_summary AS (
  SELECT
    order_id,
    COUNT(*)        AS item_count,
    SUM(item_price) AS items_total
  FROM order_items
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.order_amount,
  i.item_count,
  i.items_total
FROM orders o
LEFT JOIN item_summary i ON o.order_id = i.order_id;
-- Grain stays at one row per order
```

### Before every join, ask

> "What is the grain on the left? What is the grain on the right? What will one joined row represent?"

### Common mistakes

* Using `DISTINCT` after a bad join — this hides the bug, does not fix it
* Joining two fact-like tables directly without pre-aggregation
* Not defining output grain before writing the join

---

## Pattern 11 — Set operations: EXCEPT / INTERSECT

### Problem shape

"What rows exist in source but not in target?"
"What changed between yesterday's snapshot and today's?"
"What records are common to both sets?"

### EXCEPT

Returns rows in the first query that do not appear in the second. Compares **full rows**.

```sql
-- Rows in source that are NOT in target (new or changed)
SELECT customer_id, city FROM source_snapshot
EXCEPT
SELECT customer_id, city FROM target_snapshot;
```

```
source_snapshot        target_snapshot
| customer_id | city   | | customer_id | city    |
|-------------|--------| |-------------|---------|
|      1      | Delhi  | |      1      | Delhi   |
|      2      | Pune   | |      2      | Chennai |  ← changed
|      3      | Mumbai | |             |         |  ← new in source

EXCEPT result:
| customer_id | city   |
|-------------|--------|
|      2      | Pune   |  ← because (2, Pune) ≠ (2, Chennai)
|      3      | Mumbai |  ← not in target at all
```

**Key point:** EXCEPT is row-based. If even one column differs, the entire row appears in the result. This makes it powerful for multi-column change detection.

### INTERSECT

Returns rows that appear in **both** queries.

```sql
-- Rows that are the same in both snapshots (unchanged)
SELECT customer_id, city FROM source_snapshot
INTERSECT
SELECT customer_id, city FROM target_snapshot;
-- Returns: (1, Delhi)
```

### Use in SCD2 change detection

```sql
-- All source rows that differ from the current dim state:
SELECT customer_id, city, segment
FROM source_customers

EXCEPT

SELECT customer_id, city, segment
FROM dim_customers
WHERE is_current = 1;
```

---

# Part 5 — Choosing the Right Tool

---

## DISTINCT vs GROUP BY vs window functions

Many people know all three but do not know which one matches the problem shape. Wrong SQL can still run — but solves the wrong problem.

### The decision

| Question | Tool |
|---|---|
| Do I want fewer rows (aggregated summary)? | `GROUP BY` |
| Do I want the same rows plus group-level metrics? | Window function |
| Do I just want duplicate output rows removed (no computation)? | `DISTINCT` |

### DISTINCT

```sql
-- Remove duplicate output rows only — no business logic
SELECT DISTINCT user_id, page FROM events;
```

Returns unique (user_id, page) combinations. Does not compute anything.

**Trap:** `DISTINCT` after a bad join is not a fix. It hides a grain/cardinality bug.

### GROUP BY

```sql
-- Collapse rows into one per group and aggregate
SELECT user_id, COUNT(*) AS page_views
FROM events
GROUP BY user_id;
```

Returns **fewer rows** than the input. Employee-level detail is lost.

### Window function

```sql
-- Keep all original rows AND attach group-level info
SELECT
  user_id,
  page,
  COUNT(*) OVER (PARTITION BY user_id) AS user_page_views
FROM events;
```

Returns the **same number of rows** as input, each with group-level context attached.

### Common interview trap

"Return each employee row along with the department average salary."

```sql
-- WRONG: loses individual rows
SELECT dept, AVG(salary) FROM employees GROUP BY dept;

-- CORRECT: keeps all rows
SELECT
  emp_id,
  dept,
  salary,
  AVG(salary) OVER (PARTITION BY dept) AS dept_avg_salary
FROM employees;
```

---

## Correlated subqueries vs window functions

A correlated subquery re-executes for each outer row. Not always wrong — but many analytics problems are cleaner and more scalable with windows.

### Correlated subquery

```sql
SELECT e.emp_id, e.dept, e.salary
FROM employees e
WHERE e.salary > (
  SELECT AVG(e2.salary)
  FROM employees e2
  WHERE e2.dept = e.dept
);
-- For each employee row, recompute dept average. Conceptually correct.
```

### Window version

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
-- Computes dept average once as a window metric. Cleaner and extensible.
```

### Another example: latest row per user

```sql
-- Correlated subquery — may return multiple rows on ties
SELECT o.*
FROM orders o
WHERE o.order_time = (
  SELECT MAX(o2.order_time)
  FROM orders o2
  WHERE o2.customer_id = o.customer_id
);

-- Window version — deterministic, handles ties
SELECT *
FROM orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY order_time DESC, order_id DESC
) = 1;
```

### Mental model

If the problem says:
* compare a row to its group metric
* rank within a group
* attach prior/next/group context to each row

→ think window function first.

---

# Part 6 — Join Cardinality Deep Dive

---

## Cardinality types

| Type | Description |
|---|---|
| One-to-one | One row in A matches at most one in B. Grain preserved. |
| One-to-many | One row in A matches many in B. Left expands. |
| Many-to-one | Many in A match one in B. Grain preserved. |
| Many-to-many | Many in A match many in B. **Danger zone.** |

---

## Cardinality estimation habit

Before running any join:

* Is the join key unique on the **left** side?
* Is the join key unique on the **right** side?
* What will one joined row represent?

```
customers: 1M rows, unique by customer_id
orders: 20M rows, many per customer

→ customers JOIN orders will produce ~20M rows, not 1M
→ Any aggregation on customer-level columns will be wrong without pre-aggregation
```

---

## Full multi-table fan-out example

```
orders:       order_id | customer_id | order_amount
order_items:  order_id | item_id
item_discounts: item_id | discount_pct
```

Joining all three naively:

* orders → order_items: order_amount gets repeated per item
* order_items → item_discounts: already-repeated amount gets repeated again per discount

Result: order_amount inflated by `n_items × n_discounts_per_item`.

Fix: pre-aggregate each table to the grain you need before joining.

```sql
WITH order_totals AS (
  SELECT
    order_id,
    SUM(item_price * (1 - discount_pct)) AS net_amount
  FROM order_items oi
  JOIN item_discounts id ON oi.item_id = id.item_id
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.customer_id,
  ot.net_amount
FROM orders o
LEFT JOIN order_totals ot ON o.order_id = ot.order_id;
```

---

# Part 7 — SCD2: Detection, Full Write, Late Data, dbt Bridge

---

## What SCD2 is

SCD2 = Slowly Changing Dimension Type 2.

When a tracked attribute changes (e.g. customer city changes), you do **not** overwrite the old row. You close it and insert a new one. This preserves full history.

**The most tested DE topic at 15–18 LPA.** Write this from scratch in 20 minutes, no reference.

---

## Dimension table structure

```sql
CREATE TABLE dim_customers (
  customer_key  SERIAL PRIMARY KEY,     -- surrogate key (DW identity)
  customer_id   INT NOT NULL,           -- business / natural key
  city          VARCHAR(100),
  segment       VARCHAR(50),
  valid_from    DATE NOT NULL,
  valid_to      DATE NOT NULL DEFAULT '9999-12-31',
  is_current    SMALLINT NOT NULL DEFAULT 1
);
```

**Rules:**
* `is_current = 1` and `valid_to = 9999-12-31` → the active row
* When an attribute changes: close the old row, insert a new one
* `customer_key` (surrogate) is the FK in fact tables — never `customer_id`

```
Example state after 2 changes for customer_id = 1:
| customer_key | customer_id | city   | valid_from | valid_to   | is_current |
|--------------|-------------|--------|------------|------------|------------|
|      10      |      1      | Delhi  | 2025-01-01 | 2025-03-14 |     0      |
|      11      |      1      | Pune   | 2025-03-15 | 2025-06-30 |     0      |
|      12      |      1      | Mumbai | 2025-07-01 | 9999-12-31 |     1      |
```

---

## Step 1 — Detect changed rows

### Method A — join-based comparison (flexible, multi-column)

```sql
SELECT
  s.customer_id,
  s.city    AS new_city,
  d.city    AS old_city,
  s.segment AS new_segment,
  d.segment AS old_segment
FROM source_customers s
JOIN dim_customers d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE s.city    IS DISTINCT FROM d.city
   OR s.segment IS DISTINCT FROM d.segment;
```

### Method B — EXCEPT (elegant for full-row comparison)

```sql
-- Source rows that differ from current dim state (changed OR brand new)
SELECT customer_id, city, segment
FROM source_customers

EXCEPT

SELECT customer_id, city, segment
FROM dim_customers
WHERE is_current = 1;
```

### Method C — MD5 hashdiff (cleanest for many tracked columns)

```sql
WITH src AS (
  SELECT
    customer_id,
    city, segment, region, tier,
    MD5(CONCAT_WS('|', city, segment, region, tier)) AS src_hash
  FROM source_customers
),
tgt AS (
  SELECT
    customer_id,
    MD5(CONCAT_WS('|', city, segment, region, tier)) AS tgt_hash
  FROM dim_customers
  WHERE is_current = 1
)
SELECT s.customer_id
FROM src s
JOIN tgt t ON s.customer_id = t.customer_id
WHERE s.src_hash <> t.tgt_hash;
-- One hash comparison instead of N column comparisons.
```

---

## Step 2 — Full SCD2 write: Postgres

```sql
-- STEP 1: Close changed rows
-- Set valid_to = yesterday, is_current = 0 for rows in dim that have changed in source
UPDATE dim_customers
SET
  valid_to   = CURRENT_DATE - INTERVAL '1 day',
  is_current = 0
WHERE customer_id IN (
  -- detect changed records
  SELECT s.customer_id
  FROM source_customers s
  JOIN dim_customers d
    ON s.customer_id = d.customer_id
   AND d.is_current = 1
  WHERE s.city    IS DISTINCT FROM d.city
     OR s.segment IS DISTINCT FROM d.segment
)
AND is_current = 1;

-- STEP 2: Insert new active rows for changed + brand-new records
INSERT INTO dim_customers (customer_id, city, segment, valid_from, valid_to, is_current)
SELECT
  s.customer_id,
  s.city,
  s.segment,
  CURRENT_DATE   AS valid_from,
  '9999-12-31'   AS valid_to,
  1              AS is_current
FROM source_customers s
LEFT JOIN dim_customers d
  ON s.customer_id = d.customer_id
 AND d.is_current = 1
WHERE d.customer_id IS NULL              -- brand new (no active row in dim)
   OR s.city    IS DISTINCT FROM d.city  -- changed
   OR s.segment IS DISTINCT FROM d.segment;
```

In plain English:
* Step 1: mark old versions as history
* Step 2: open fresh active versions for everything new or changed

---

## Step 3 — SCD2 via MERGE: Snowflake

Single MERGE cannot atomically close old rows AND insert new ones. The standard Snowflake approach uses two passes:

```sql
-- Pass 1: close rows that changed
MERGE INTO dim_customers tgt
USING (
  SELECT s.customer_id, s.city, s.segment
  FROM source_customers s
  JOIN dim_customers d
    ON s.customer_id = d.customer_id
   AND d.is_current = 1
  WHERE s.city IS DISTINCT FROM d.city
     OR s.segment IS DISTINCT FROM d.segment
) changed
ON tgt.customer_id = changed.customer_id
AND tgt.is_current = 1
WHEN MATCHED THEN UPDATE SET
  tgt.valid_to   = CURRENT_DATE - 1,
  tgt.is_current = 0;

-- Pass 2: insert new active rows for changed + new records
MERGE INTO dim_customers tgt
USING source_customers src
ON tgt.customer_id = src.customer_id AND tgt.is_current = 1
WHEN NOT MATCHED THEN INSERT
  (customer_id, city, segment, valid_from, valid_to, is_current)
  VALUES (src.customer_id, src.city, src.segment, CURRENT_DATE, '9999-12-31', 1)
WHEN MATCHED AND (src.city IS DISTINCT FROM tgt.city OR src.segment IS DISTINCT FROM tgt.segment)
  THEN INSERT
  (customer_id, city, segment, valid_from, valid_to, is_current)
  VALUES (src.customer_id, src.city, src.segment, CURRENT_DATE, '9999-12-31', 1);
```

---

## Step 4 — Late-arriving records

### The problem

Your pipeline runs at midnight for `event_date = yesterday`. Three days later, a record arrives with `event_date = three days ago` — delayed by a mobile sync or upstream retry.

Strict watermark logic already moved past that date. The late record is silently missed.

### Fix 1 — Lookback window (most practical)

```sql
-- Instead of: WHERE updated_at > :last_watermark
-- Use:
WHERE updated_at >= :last_watermark - INTERVAL '3 days'
```

Picks up records arriving up to 3 days late. Use MERGE downstream with a business key so reprocessing is idempotent.

### Fix 2 — Late-arrival SQL for SCD2

If the late record falls inside an already-closed SCD2 window:

```sql
-- Scenario: city changed on 2025-03-10 (effective date)
-- but record arrived on 2025-03-13
-- Dim currently shows Delhi active from 2025-02-01

-- Step 1: split the existing row at the effective date
UPDATE dim_customers
SET valid_to = '2025-03-09'  -- day before effective date
WHERE customer_id = 1
  AND is_current = 1
  AND valid_from < '2025-03-10';

-- Step 2: insert the late-arriving version
INSERT INTO dim_customers (customer_id, city, valid_from, valid_to, is_current)
VALUES (1, 'Mumbai', '2025-03-10', '9999-12-31', 1);
```

### Trade-off table

| Approach | Pro | Con |
|---|---|---|
| Lookback window | Simple, catches most late data | Small cost increase per run |
| Full partition reprocess | Always correct | Expensive for large tables |
| Event-time vs ingestion-time | Architecturally clean | Requires event_time in all sources |
| Accept late arrivals up to N days | Predictable SLA | Some records permanently missed |

### Verbal answer

> "Late-arriving records require a lookback window — I reprocess the last N days instead of strictly watermark-forward. Downstream I use MERGE with a business key so reprocessing is idempotent. For SCD2 specifically, if a record falls inside an already-closed dim window, I split the existing row at the correct effective date and insert the late version."

---

## Step 5 — dbt snapshot: the production bridge

In production with a modern stack, SCD2 is usually implemented via **dbt snapshots**, not raw SQL.

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}
  {{
    config(
      target_schema = 'snapshots',
      unique_key    = 'customer_id',
      strategy      = 'timestamp',
      updated_at    = 'updated_at'
    )
  }}
  SELECT * FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
```

When `dbt snapshot` runs, dbt automatically:
* detects changed rows using `updated_at`
* closes old rows: sets `dbt_valid_to`
* inserts new rows: sets `dbt_valid_from` = current timestamp
* adds `dbt_scd_id` (unique key per version)

**Two strategies:**
* `strategy: timestamp` — uses an `updated_at` column. Fast. Use when source has a reliable timestamp.
* `strategy: check` — compares specific columns. Use when source has no reliable `updated_at`.

### Interview framing

> "I'd implement SCD2 via a dbt snapshot in the warehouse layer — it generates dbt_valid_from and dbt_valid_to automatically. If I were writing raw SQL outside dbt, I'd use UPDATE to close changed rows followed by INSERT for new versions, using IS DISTINCT FROM for null-safe comparison. Both paths implement the same logic: detect change → close old row → open new row."

Being able to say both paths in one answer is what distinguishes you at 15–18 LPA.

---

# Part 8 — MERGE: Full Reference

---

## What MERGE does

MERGE combines INSERT, UPDATE, and DELETE into one statement based on a join condition.

Standard tool for:
* SCD2 implementation
* Upsert / idempotent load patterns
* Incremental pipeline loads
* CDC-style target maintenance

---

## Core syntax — Postgres (15+)

```sql
MERGE INTO target_table tgt
USING source_table src
ON tgt.id = src.id

WHEN MATCHED THEN
  UPDATE SET
    tgt.col1 = src.col1,
    tgt.col2 = src.col2

WHEN NOT MATCHED THEN
  INSERT (id, col1, col2)
  VALUES (src.id, src.col1, src.col2);
```

---

## Core syntax — Snowflake

```sql
MERGE INTO target_table tgt
USING source_table src
ON tgt.id = src.id

WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
  UPDATE SET
    tgt.col1     = src.col1,
    tgt.row_hash = src.row_hash

WHEN NOT MATCHED THEN
  INSERT (id, col1, row_hash)
  VALUES (src.id, src.col1, src.row_hash)

WHEN NOT MATCHED BY SOURCE THEN
  DELETE;
-- WHEN NOT MATCHED BY SOURCE: Snowflake-specific.
-- Deletes target rows that no longer exist in source.
-- Postgres does NOT support this.
```

---

## Snowflake vs Postgres: key differences

| Feature | Snowflake | Postgres |
|---|---|---|
| `WHEN MATCHED` | Yes | Yes |
| `WHEN NOT MATCHED` | Yes | Yes |
| `WHEN NOT MATCHED BY SOURCE` | Yes — rows in target not in source | No |
| Multiple `WHEN MATCHED` clauses | Yes | Yes (Postgres 15+) |
| INSERT inside `WHEN MATCHED` | Yes (used for SCD2 insert-only trick) | No |

---

## Idempotent MERGE

Running the same MERGE twice must produce the same result. This is what makes pipelines safe to retry.

```sql
-- Run this for the same source data twice.
-- Row count in target must be identical both times.
MERGE INTO orders_target tgt
USING orders_source src
ON tgt.order_id = src.order_id

WHEN MATCHED AND tgt.updated_at < src.updated_at THEN
  UPDATE SET
    tgt.status     = src.status,
    tgt.updated_at = src.updated_at
  -- guard: only update if source is newer → re-running with same source does nothing

WHEN NOT MATCHED THEN
  INSERT (order_id, status, updated_at)
  VALUES (src.order_id, src.status, src.updated_at);
  -- on re-run: row now exists → matched → no second insert
```

---

## MERGE with hashdiff

```sql
MERGE INTO dim_customers tgt
USING (
  SELECT
    customer_id, city, segment,
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

## 10 MERGE scenarios

```sql
-- 1. Simple upsert
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET t.val = s.val
WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);

-- 2. Insert-only (ignore existing rows)
MERGE INTO t USING s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);

-- 3. Update-only (ignore new rows)
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.val = s.val;

-- 4. Upsert with hash guard (only update when data changed)
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED     AND t.hash <> s.hash THEN UPDATE SET t.val = s.val, t.hash = s.hash
WHEN NOT MATCHED THEN INSERT (id, val, hash) VALUES (s.id, s.val, s.hash);

-- 5. Soft-delete (mark inactive instead of deleting) — Snowflake
MERGE INTO t USING s ON t.id = s.id
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.is_active = 0;

-- 6. Hard-delete — Snowflake
MERGE INTO t USING s ON t.id = s.id
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- 7. Conditional update (only update when a flag is set)
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED AND s.override_flag = 1 THEN UPDATE SET t.val = s.val;

-- 8. Idempotent partition load (delete + insert pattern — not MERGE syntax but equivalent)
DELETE FROM target_orders WHERE order_date = :run_date;
INSERT INTO target_orders SELECT * FROM source_orders WHERE order_date = :run_date;
-- Run twice → identical result

-- 9. SCD2 close pass — close changed rows
MERGE INTO dim_customers tgt
USING changed_records src
ON tgt.customer_id = src.customer_id AND tgt.is_current = 1
WHEN MATCHED THEN UPDATE SET tgt.valid_to = CURRENT_DATE - 1, tgt.is_current = 0;

-- 10. Timestamp-guarded upsert
MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED     AND t.updated_at < s.updated_at THEN UPDATE SET t.val = s.val, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, val, updated_at) VALUES (s.id, s.val, s.updated_at);
```

---

## dbt incremental strategy: merge vs delete+insert

```sql
-- dbt incremental model example
{{ config(
    materialized        = 'incremental',
    unique_key          = 'order_id',
    incremental_strategy= 'merge'
) }}

SELECT order_id, status, updated_at
FROM {{ source('raw', 'orders') }}
{% if is_incremental() %}
  WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

| Use `merge` when | Use `delete+insert` when |
|---|---|
| Rows update in place (CDC style) | You replace entire date partitions |
| You want precise row-level upserts | Simple, clean partition swap |
| Source sends deltas | Source sends full daily snapshots |

---

# Part 9 — Incremental Processing, Watermarking, and Idempotency

---

## Why incremental processing exists

If a table has billions of rows, you do not want to recompute everything every day. Process only new or changed data to reduce runtime, cost, and failure surface.

---

## Three core strategies

### 1. Full refresh

Rebuild entire target every run.

Good when: data is small, logic is simple, early model stage, correctness matters more than cost.
Bad when: data is huge or SLA is tight.

### 2. Incremental append

Load only newly arrived rows.

```sql
SELECT *
FROM source_events
WHERE event_time > (SELECT MAX(event_time) FROM target_events);
```

Simple but fragile — misses late-arriving data if source is not perfectly ordered.

### 3. Incremental upsert / MERGE

Insert new rows and update changed rows. See Part 8 (MERGE). Standard for dimensions and latest-state tables.

---

## Watermarking

A watermark is the "how far have I processed?" marker.

```sql
SELECT *
FROM source_orders
WHERE updated_at > :last_watermark;
```

**Why watermarks are tricky:**

Suppose your last run processed up to `2025-01-10 10:00:00`. A late record arrives with `updated_at = 2025-01-10 09:55:00`. Strict `>` watermark misses it.

**Fix: lookback window**

```sql
SELECT *
FROM source_orders
WHERE updated_at >= :last_watermark - INTERVAL '1 day';
```

Then deduplicate downstream with MERGE or `unique_key` — so reprocessing existing records is safe.

---

## Idempotency

> Running the same pipeline again must not create incorrect duplicates or inconsistent results.

**Non-idempotent:**

```sql
INSERT INTO target_orders
SELECT * FROM source_orders
WHERE order_date = CURRENT_DATE;
-- Re-run: same rows inserted again → duplicates
```

**Idempotent patterns:**

```sql
-- Pattern A: MERGE with business key
MERGE INTO target_orders tgt USING source_orders src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Pattern B: DELETE + INSERT (partition replacement)
DELETE FROM target_orders WHERE order_date = :run_date;
INSERT INTO target_orders SELECT * FROM source_orders WHERE order_date = :run_date;
-- Both patterns produce identical results on any number of re-runs
```

---

## Mental model for recurring SQL pipelines

Before designing any incremental SQL, answer:

1. What identifies a row uniquely?
2. What marks new or changed data?
3. Can data arrive late?
4. If the job reruns, will I duplicate data?
5. When should I rebuild fully instead?

That is data engineering thinking.

---

## Common interview questions on this topic

* Full refresh vs incremental: when would you choose each?
* How do you handle late-arriving data?
* How do you make an incremental load idempotent?
* Why is watermark-only loading risky?
* What is the difference between MERGE and delete+insert?

---

# Part 10 — Cohort Analysis

---

## What it is

Group users by when they first did something (first purchase, first login, first activation). Then track their behaviour in subsequent periods relative to that starting point.

Standard product analytics pattern. Appears on DataLemur and StrataScratch.

---

## Standard cohort retention query

**Problem:** given `orders(user_id, order_date)`, compute retention — what percentage of users from each monthly cohort came back to purchase in subsequent months.

```sql
WITH first_order AS (
  -- Each user's cohort = month of their very first order
  SELECT
    user_id,
    DATE_TRUNC('month', MIN(order_date)) AS cohort_month
  FROM orders
  GROUP BY user_id
),
user_activity AS (
  -- Join all orders back to cohort label
  SELECT
    o.user_id,
    f.cohort_month,
    DATE_TRUNC('month', o.order_date) AS activity_month
  FROM orders o
  JOIN first_order f ON o.user_id = f.user_id
),
cohort_data AS (
  -- Compute period index:
  --   period 0 = same month as cohort
  --   period 1 = one month later, etc.
  SELECT
    cohort_month,
    activity_month,
    (
      EXTRACT(YEAR  FROM AGE(activity_month, cohort_month)) * 12
      + EXTRACT(MONTH FROM AGE(activity_month, cohort_month))
    )::INT                   AS period_number,
    COUNT(DISTINCT user_id)  AS active_users
  FROM user_activity
  GROUP BY cohort_month, activity_month
)
SELECT
  cohort_month,
  period_number,
  active_users,
  -- Cohort size = period 0 count — FIRST_VALUE picks it for all rows in the cohort
  FIRST_VALUE(active_users) OVER (
    PARTITION BY cohort_month
    ORDER BY period_number
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                          AS cohort_size,
  ROUND(
    100.0 * active_users
    / FIRST_VALUE(active_users) OVER (
        PARTITION BY cohort_month
        ORDER BY period_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
    1
  )                          AS retention_pct
FROM cohort_data
ORDER BY cohort_month, period_number;
```

### Sample output

```
| cohort_month | period_number | active_users | cohort_size | retention_pct |
|--------------|---------------|--------------|-------------|---------------|
| 2025-01-01   |       0       |     500      |     500     |    100.0      |
| 2025-01-01   |       1       |     310      |     500     |     62.0      |
| 2025-01-01   |       2       |     220      |     500     |     44.0      |
| 2025-02-01   |       0       |     450      |     450     |    100.0      |
| 2025-02-01   |       1       |     270      |     450     |     60.0      |
```

### How to think about it

* period 0 = same month as cohort → always 100% by definition
* `FIRST_VALUE` window picks the period-0 `active_users` count as denominator for all rows in that cohort
* Patterns used: `DATE_TRUNC` + aggregation + `AGE` for period index + `FIRST_VALUE` window

### Snowflake simplification

```sql
-- Instead of AGE():
DATEDIFF('month', cohort_month, activity_month) AS period_number
```

---

# Part 11 — Partition Pruning and Warehouse-Aware SQL

---

## The shift from OLTP to warehouse thinking

In OLTP: "use an index."

In cloud warehouses: prune micro-partitions / partitions, scan fewer files, align filters with storage layout.

---

## Partition pruning

The engine skips irrelevant data chunks when filters align with how data is stored.

```sql
-- GOOD: engine reads only January micro-partitions
SELECT *
FROM sales
WHERE order_date >= DATE '2025-01-01'
  AND order_date < DATE '2025-02-01';

-- BAD: function on filter column → engine must scan everything
SELECT *
FROM sales
WHERE YEAR(order_date) = 2025;
-- YEAR() wraps the column → pruning is broken
-- Same problem with: DATE_TRUNC('month', order_date) = '2025-01-01'
```

**Rule:** always filter with range conditions on the raw column, never wrap it in a function.

---

## Clustering keys in Snowflake

Snowflake automatically sorts data within micro-partitions. You can define a clustering key to keep frequently-filtered columns co-located:

```sql
ALTER TABLE sales CLUSTER BY (order_date, customer_id);
```

If your largest table is frequently filtered by `order_date` and `customer_id`, those are strong candidates.

---

## Query optimisation checklist for warehouse SQL

When asked "how would you optimise a slow query on a huge fact table?":

1. Filter early — push WHERE conditions as far upstream as possible
2. Project only needed columns — columnar systems charge per column scanned
3. Avoid functions on partition/filter columns — kills pruning
4. Align filters with partition keys / clustering keys
5. Pre-aggregate before joins — prevent fan-out from expanding row counts
6. Use Snowflake Query Profile — identify which stage scans the most data

### Interview answer

> "I first look at whether the filter column is wrapped in a function — that kills partition pruning in warehouses. Then I check whether any joins are causing row count explosions before aggregation. And I look at the Query Profile in Snowflake to find the most expensive step. The core question is: can the engine skip most of the data, or am I forcing it to scan everything?"

---

# Part 12 — Pattern Combinations and Multi-Pattern Examples

---

## Why hard problems feel hard

The nastiest SQL questions combine 2–3 patterns. Recognising the pattern composition is the skill.

| Problem | Patterns needed |
|---|---|
| Longest paid subscription streak per user | gaps & islands + filtering + ranking |
| Latest active plan excluding cancelled ones | filter + dedup latest-row + anti-join |
| Count sessions per user per day | sessionisation + date extract + aggregation |
| Find products never sold after launch | anti-join + time filter |
| Top 3 longest inactivity gaps per customer | LAG + interval calc + ranking |
| Find revenue inflated by multi-item orders | fan-out detection + pre-aggregation |
| Cohort retention | first-event date + period index + window ratio |
| SCD2 delta load | EXCEPT/hashdiff + UPDATE/INSERT + idempotency |

---

## Full example 1 — Longest consecutive login streak per user

Patterns: dedup → row numbering → island labelling → aggregation → final ranking

```sql
WITH deduped AS (
  -- protect against duplicate login records for same user-date
  SELECT DISTINCT user_id, login_date
  FROM user_logins
),
numbered AS (
  SELECT
    user_id,
    login_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY login_date
    ) AS rn
  FROM deduped
),
islands AS (
  -- date - rn stays constant within a consecutive run
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
    COUNT(*)        AS streak_len
  FROM islands
  GROUP BY user_id, grp
),
ranked_streaks AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id
      ORDER BY streak_len DESC, streak_end DESC  -- longest; tie-break by most recent
    ) AS rn
  FROM streaks
)
SELECT user_id, streak_start, streak_end, streak_len
FROM ranked_streaks
WHERE rn = 1
ORDER BY user_id;
```

```
Input: user 1 logged in on Jan 1-3 and Jan 5-6. user 2 logged in Feb 1 and Feb 3.

Output:
| user_id | streak_start | streak_end | streak_len |
|---------|--------------|------------|------------|
|    1    | 2025-01-01   | 2025-01-03 |     3      |
|    2    | 2025-02-01   | 2025-02-01 |     1      |
```

---

## Full example 2 — Session summaries with multiple users

Patterns: LAG → boundary detection → cumulative SUM → aggregation

```sql
WITH lagged AS (
  SELECT
    user_id,
    event_time,
    event_name,
    LAG(event_time) OVER (
      PARTITION BY user_id
      ORDER BY event_time
    ) AS prev_time
  FROM events
),
boundaries AS (
  SELECT
    *,
    CASE
      WHEN prev_time IS NULL                                   THEN 1  -- first event
      WHEN event_time - prev_time > INTERVAL '30 minutes'     THEN 1  -- gap > threshold
      ELSE 0
    END AS is_new_session
  FROM lagged
),
with_session_id AS (
  SELECT
    *,
    SUM(is_new_session) OVER (
      PARTITION BY user_id
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM boundaries
)
SELECT
  user_id,
  session_id,
  MIN(event_time)                          AS session_start,
  MAX(event_time)                          AS session_end,
  COUNT(*)                                 AS event_count,
  MAX(event_time) - MIN(event_time)        AS session_duration
FROM with_session_id
GROUP BY user_id, session_id
ORDER BY user_id, session_id;
```

---

## Full example 3 — Revenue analysis avoiding fan-out (three tables)

```sql
-- Problem: compute net revenue per customer from orders, order_items, and item_discounts
-- Naive join explodes order_amount across multiple items and discounts

WITH item_net AS (
  -- pre-aggregate to order grain: net amount per order
  SELECT
    oi.order_id,
    SUM(oi.item_price * (1 - COALESCE(id.discount_pct, 0))) AS net_order_amount,
    COUNT(oi.item_id)                                        AS item_count
  FROM order_items oi
  LEFT JOIN item_discounts id ON oi.item_id = id.item_id
  GROUP BY oi.order_id
),
order_level AS (
  SELECT
    o.customer_id,
    o.order_id,
    o.order_date,
    i.net_order_amount,
    i.item_count
  FROM orders o
  JOIN item_net i ON o.order_id = i.order_id
)
SELECT
  customer_id,
  COUNT(DISTINCT order_id)     AS total_orders,
  SUM(item_count)              AS total_items,
  SUM(net_order_amount)        AS total_net_revenue,
  ROUND(AVG(net_order_amount), 2) AS avg_order_value
FROM order_level
GROUP BY customer_id
ORDER BY total_net_revenue DESC;
```

---

# Part 13 — How to Attack SQL Problems in Interviews

---

## The 5-step process

### Step 1: Restate the grain

"Does one row represent an event, an order, or a customer snapshot?"

### Step 2: Clarify output

"Do you want one row per customer, per session, or per day?"

### Step 3: Clarify ties and duplicates

"If two rows have the same timestamp, how should I break the tie?"

### Step 4: Identify the pattern

* dedup / latest row → ROW_NUMBER
* ranking → DENSE_RANK / RANK
* adjacent row comparison → LAG / LEAD
* sequence grouping → gaps & islands / sessionisation
* anti-join → NOT EXISTS
* set difference → EXCEPT
* history management → SCD2

### Step 5: Build in stages with CTEs

Do not write one giant query. Good interview SQL is:

* CTE 1 = annotate rows (add LAG, ROW_NUMBER, flags)
* CTE 2 = derive intermediate values (island labels, session IDs, change flags)
* CTE 3 = aggregate / rank / filter

That is how readable production SQL is written too.

---

## The 7-step mental checklist

For any problem, run through this before writing:

1. What does one input row represent?
2. What should one output row represent?
3. What is the entity key (per user, per order, per dept)?
4. Does row order matter? (need ranking, LAG, running logic, boundaries?)
5. What is the cardinality of each join?
6. Can NULL break any comparison, join, filter, or aggregation?
7. What is the right tool: GROUP BY, window, anti-join, set op, MERGE, or pre-aggregate?

---

## Real-world DE usage of each pattern

| Pattern | Where it appears in DE work |
|---|---|
| Dedup latest row | CDC ingestion, bronze → silver, dim snapshots |
| Anti-join | Incremental loads, missing record detection, reconciliation |
| Sessionisation | Clickstream analytics, user behaviour pipelines |
| Gaps and islands | Uptime streaks, attendance, consecutive trading days |
| Set difference | Source vs target validation, DQ checks, SCD2 detection |
| Fan-out detection | Debugging inflated metrics, preventing broken marts |
| SCD2 | Customer/product/account dimension history |
| Incremental + MERGE | Every daily pipeline in production |

---

# Part 14 — What to Memorise vs What to Understand

---

## Must memorise — write from memory, no lookup

### Latest row per key (Postgres)

```sql
ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC)
-- then WHERE rn = 1
```

### Latest row per key (Snowflake)

```sql
QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) = 1
```

### Top N distinct values per group

```sql
DENSE_RANK() OVER (PARTITION BY grp ORDER BY metric DESC)
-- then WHERE rnk <= N
```

### Running total

```sql
SUM(val) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

### 7-day moving average

```sql
AVG(val) OVER (ORDER BY ts ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```

### Sessionisation skeleton

```
LAG(ts) → gap flag → SUM(gap_flag) OVER (PARTITION BY key ORDER BY ts ROWS UNBOUNDED PRECEDING)
```

### Gaps and islands skeleton

```
date - ROW_NUMBER() OVER (PARTITION BY key ORDER BY date)  →  constant per island
```

### Anti-join

```sql
WHERE NOT EXISTS (SELECT 1 FROM right_table r WHERE r.key = l.key)
```

### SCD2 write skeleton

```
UPDATE dim SET valid_to = yesterday, is_current = 0 WHERE changed
INSERT INTO dim (... valid_from = today, valid_to = 9999, is_current = 1) WHERE changed OR new
```

### Idempotent partition load

```sql
DELETE FROM target WHERE dt = :run_date;
INSERT INTO target SELECT * FROM source WHERE dt = :run_date;
```

---

## Must understand deeply — concept, not rote

* Why ROW_NUMBER cannot be filtered in WHERE (execution order)
* Why QUALIFY exists and where it works
* ROWS vs RANGE — what changes on tied values
* NULL = NULL is not true — three-valued logic
* NOT IN breaks when subquery has NULLs
* IS DISTINCT FROM — when and why to use it
* Fan-out — why SUM inflates after a join to a many-side table
* DISTINCT is not a fix for a bad join
* SCD2 logic — close + insert, surrogate key vs business key
* Late-arriving data — lookback window + idempotent MERGE
* Full refresh vs incremental — when to use each
* Idempotency — what it means and how to prove it
* Partition pruning — why function-wrapped filter columns break it
* dbt snapshot = automated SCD2
* dbt incremental merge vs delete+insert — when each

---

# Part 15 — Interview Verbal Answers

---

## On advanced SQL generally

> "For advanced SQL problems, I first define the grain of the input and the required grain of the output, because most mistakes come from grain mismatch or join fan-out. Then I check whether the problem is aggregation, row preservation with window functions, or a set/existence problem like an anti-join. I'm careful about NULL semantics too — NOT IN, joins, and comparisons can silently behave differently when NULLs are present. For ranking and latest-row problems I use ROW_NUMBER, then filter with a CTE or QUALIFY in Snowflake. In production DE work I also think about incremental loading, idempotency, partition pruning, and whether joins will multiply rows or scan too much data."

## On pattern identification

> "Advanced SQL problems usually reduce to a few reusable patterns. I identify the grain, the grouping key, and whether row order matters — then map the problem to a pattern: deduplication with ROW_NUMBER, change detection with LAG, sessionisation with LAG plus cumulative SUM, gaps-and-islands with the row-number subtraction trick, or anti-joins with NOT EXISTS. In production DE these patterns appear in CDC deduping, session analytics, incremental loads, reconciliation, and SCD2 history tracking. I solve them in stages with CTEs so the logic stays correct and explainable."

## On SCD2

> "I'd implement SCD2 via a dbt snapshot in the warehouse layer — it generates dbt_valid_from and dbt_valid_to automatically. If I were outside dbt, I'd write raw SQL: first UPDATE to close changed rows by setting valid_to and is_current=0, then INSERT fresh active rows for anything changed or brand new, using IS DISTINCT FROM for null-safe comparison. For late-arriving records I use a lookback window so records arriving a few days late still get picked up, and MERGE downstream ensures the re-processing is idempotent."

## On ROWS vs RANGE

> "I default to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW for running totals because it operates on physical row positions. RANGE is value-based and groups tied rows together, so if your ORDER BY column has duplicates, the running total can jump unexpectedly. ROWS gives you deterministic, row-by-row accumulation."

## On anti-joins

> "I usually write anti-joins as NOT EXISTS — it expresses intent clearly, it's robust when nullable keys exist, and the engine can typically short-circuit as soon as it finds a match. LEFT JOIN IS NULL is also valid, but I avoid NOT IN unless I've confirmed the subquery column cannot return NULLs."

## On idempotency

> "Idempotency means running the same pipeline twice produces the same result. In practice I use MERGE with a business key — existing rows update only if the source is newer, new rows insert once. For partition-replacement loads I do DELETE WHERE dt equals the run date, then INSERT, so re-running is always a clean overwrite."

---

# Part 16 — Practice Problems

---

## Easy-medium

**1.** Given `customer_orders(customer_id, order_id, order_time)`, return the latest order per customer. Write both the CTE version and the Snowflake QUALIFY version.

**2.** Given `employees(department, employee_id, salary)`, return the top 3 distinct salary values per department. Show what changes if you use ROW_NUMBER instead of DENSE_RANK.

**3.** Given `product_prices(product_id, price_date, price)`, return rows where the price changed from the previous day. Handle the case where price can be NULL.

**4.** Given `daily_sales(sale_date, amount)`, compute: running total, 7-day moving average. Make sure ROWS is explicit.

---

## Medium

**5.** Why does this fail? Fix it two ways (CTE and QUALIFY):

```sql
SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
FROM orders WHERE rn = 1;
```

**6.** Given `customers` and `orders`, return customers with no orders using both NOT EXISTS and LEFT JOIN IS NULL. Explain which you prefer and why. Then show why NOT IN is dangerous if orders.customer_id can be NULL.

**7.** Given `sales(day, amount)` with duplicate day values, show the difference in output between ROWS and RANGE running totals. Explain the tie behaviour.

**8.** Given `user_logins(user_id, login_date)`, return all consecutive login streaks per user with start date, end date, and streak length.

---

## Hard

**9.** From the same login table, return only the longest streak per user. Handle users who might have logged in twice on the same day.

**10.** Given `events(user_id, event_time, event_name)`, assign session IDs where inactivity > 30 minutes. Then aggregate to session summaries: session start, end, duration, event count.

**11.** Given `orders(order_id, customer_id, order_amount)`, `order_items(order_id, item_id, item_price)`, and `item_discounts(item_id, discount_pct)`, compute correct net revenue per customer without inflating order_amount. Show the naive join bug explicitly first.

**12.** Given `source_customers(customer_id, city, segment)` and `dim_customers` (with SCD2 structure), write the full SCD2 load: detect changes using both EXCEPT and MD5 hashdiff, close old rows, insert new rows.

**13.** Write an idempotent MERGE for a `target_orders` table. Run it twice for the same source data. Explain why row count is identical both times.

**14.** Design an incremental load for `source_events(event_id, updated_at, event_type)` into `target_events`. Cover: watermark, late-arriving data, idempotency. Write the actual SQL.

**15.** A query on a huge `events` table is slow: `WHERE YEAR(event_date) = 2025`. Explain why, rewrite it, and explain what partition pruning means in Snowflake's micro-partition model.

**16.** Write a cohort retention query. Given `orders(user_id, order_date)`, compute per-cohort monthly retention percentage. Show the full query including the period index and FIRST_VALUE denominator.

---

# Part 17 — Study Schedule and Final Checklist

---

## 7-day study loop

**Day 1**
* Latest row per key (CTE + QUALIFY)
* Top N per group (all 3 ranking functions)
* SQL execution order + why QUALIFY exists

**Day 2**
* LAG / LEAD change detection
* NULL semantics — three-valued logic, IS DISTINCT FROM, NOT IN trap

**Day 3**
* Running totals
* 7-day moving average
* ROWS vs RANGE deep dive

**Day 4**
* Gaps and islands (all streaks)
* Longest streak (layered ranking)

**Day 5**
* Sessionisation
* Anti-joins — all three patterns, NULL trap

**Day 6**
* Fan-out / cardinality — detection + pre-aggregation fix
* Set operations — EXCEPT for change detection
* SCD2 — full write SQL cold from scratch

**Day 7**
* Multi-pattern problems combining 2–3 patterns
* MERGE scenarios 1–10
* Cohort retention query

---

## Patterns to write cold — daily checklist

- [ ] Latest row — ROW_NUMBER + CTE (Postgres)
- [ ] Latest row — QUALIFY (Snowflake)
- [ ] Top N — DENSE_RANK
- [ ] Change detection — LAG + IS DISTINCT FROM
- [ ] Running total — SUM OVER ROWS UNBOUNDED PRECEDING
- [ ] 7-day moving average — AVG OVER 6 PRECEDING
- [ ] Gaps and islands — date minus ROW_NUMBER trick
- [ ] Longest streak — gaps + island ranking
- [ ] Sessionisation — LAG gap flag + cumulative SUM
- [ ] Anti-join — NOT EXISTS
- [ ] Anti-join — LEFT JOIN IS NULL
- [ ] Fan-out detection — uniqueness check + row count comparison
- [ ] Pre-aggregation fix for fan-out
- [ ] EXCEPT for change detection
- [ ] SCD2 full write — UPDATE + INSERT (Postgres)
- [ ] Idempotent MERGE — business key + timestamp guard
- [ ] Idempotent partition load — DELETE + INSERT
- [ ] Cohort retention — DATE_TRUNC + FIRST_VALUE

## Concepts to explain verbally — no notes

- [ ] Why ROW_NUMBER cannot be in WHERE
- [ ] What QUALIFY does and where it works
- [ ] ROWS vs RANGE on tied values
- [ ] NULL = NULL is not true — three-valued logic
- [ ] NOT IN breaks with NULLs in subquery
- [ ] IS DISTINCT FROM — when and why
- [ ] Fan-out — why SUM inflates after a join
- [ ] DISTINCT does not fix a bad join
- [ ] SCD2 logic — plain English
- [ ] Late data — lookback window + idempotent MERGE
- [ ] Full refresh vs incremental — when each
- [ ] Idempotency — meaning + concrete example
- [ ] Partition pruning — function-wrapping kills it
- [ ] dbt snapshot = SCD2 automatically
- [ ] MERGE merge vs delete+insert — when each

---

## Final note

Advanced SQL is not about memorising 200 tricks.

It is about mastering a small set of reusable ideas:

* **rank rows** · **compare adjacent rows** · **accumulate over ordered rows**
* **label boundaries** · **group consecutive runs** · **exclude matches**
* **compare sets** · **control join cardinality** · **preserve history across changes**
* **run pipelines safely, repeatedly, correctly**

Once you can recognise those shapes and connect them to production DE reality — CDC deduping, SCD2, incremental pipelines, idempotency, warehouse performance — "tricky" SQL stops feeling magical.
