# 1) What “advanced SQL patterns” really means

These are not random tricks.

They exist because in real work and interviews, many questions are secretly one of these:

* “pick one row out of many”
* “compare this row to the previous row”
* “group events into runs or sessions”
* “find what is missing”
* “find change boundaries”
* “join safely without duplicating everything”
* “build business logic on ordered data”

So advanced SQL is mostly about learning to recognize the **shape of the problem**.

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

That is why your roadmap clusters together:

* `ROW_NUMBER / RANK / DENSE_RANK`
* `LAG / LEAD`
* frames
* latest-record-per-key
* `QUALIFY`
* anti-joins
* join explosion
* gaps and islands
* sessionization
* SCD2 change detection.

---

# 2) The master mental model

Most tricky SQL problems are solved in 4 moves:

## Move 1: define the grain

What does one row represent?

Examples:

* one order
* one user event
* one employee salary record
* one daily balance

If you get the grain wrong, everything breaks.

## Move 2: identify the grouping key

What entity are we solving this for?

Examples:

* per user
* per customer
* per product
* per department

## Move 3: identify ordering

Does row sequence matter?

Examples:

* by event_time
* by effective_date
* by transaction_id

## Move 4: choose the pattern

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

# 3) The core advanced SQL patterns you must know

These are the important ones for your roadmap and interviews:

1. **Latest row per key / dedup**
2. **Top N per group**
3. **Change detection with `LAG/LEAD`**
4. **Running totals / moving windows**
5. **Gaps and islands**
6. **Sessionization**
7. **Anti-joins**
8. **Join explosion / fan-out detection**
9. **Set difference with `EXCEPT` / `INTERSECT`**
10. **SCD2-style change detection**

Your roadmap explicitly calls out dedup, gaps and islands, sessionization, anti-joins, fan-out detection, and set operations; your interview guide emphasizes window functions, dedup, gaps & islands, and SCD2 as high-frequency SQL patterns.

Now let’s build these properly.

---

# 4) Pattern 1 - Latest row per key / deduplication

This is one of the most common interview problems.

## Problem shape

“Give me the latest row per customer.”
“Keep only the most recent status per order.”
“Deduplicate event rows.”

## Why it exists

In real DE work:

* CDC streams produce multiple versions
* raw ingestion tables contain duplicates
* dimension updates arrive over time
* interviewers use it because it tests ranking + partitioning

Your roadmap explicitly says: latest record per key = `ROW_NUMBER() + WHERE rn = 1`, and Snowflake `QUALIFY` is a shortcut worth knowing.

## Example

Table: `customer_status`

| customer_id | status | updated_at       |
| ----------- | ------ | ---------------- |
| 1           | bronze | 2025-01-01 10:00 |
| 1           | silver | 2025-01-10 09:00 |
| 1           | gold   | 2025-01-15 12:00 |
| 2           | bronze | 2025-01-03 08:00 |
| 2           | silver | 2025-01-04 08:00 |

## Query

```sql
with ranked as (
  select
    customer_id,
    status,
    updated_at,
    row_number() over (
      partition by customer_id
      order by updated_at desc
    ) as rn
  from customer_status
)
select
  customer_id,
  status,
  updated_at
from ranked
where rn = 1;
```

## Output

| customer_id | status | updated_at       |
| ----------- | ------ | ---------------- |
| 1           | gold   | 2025-01-15 12:00 |
| 2           | silver | 2025-01-04 08:00 |

## How to think about it

For each customer:

* sort rows newest to oldest
* assign row numbers
* keep first row

## Common mistake

If two rows have same `updated_at`, result may be non-deterministic.

Use a tie-breaker:

```sql
order by updated_at desc, load_id desc
```

## Snowflake version

```sql
select
  customer_id,
  status,
  updated_at
from customer_status
qualify row_number() over (
  partition by customer_id
  order by updated_at desc
) = 1;
```

That exact Snowflake shortcut is called out in your roadmap and interview guide.

---

# 5) Pattern 2 - Top N per group

## Problem shape

“Top 3 salaries per department.”
“Most recent 2 orders per customer.”
“Top 5 products by revenue per category.”

## Core idea

Use ranking functions:

* `ROW_NUMBER()` → top N rows
* `RANK()` → ties share rank, gaps exist
* `DENSE_RANK()` → ties share rank, no gaps

Your interview guide tells you to know `ROW_NUMBER`, `RANK`, and `DENSE_RANK` cold and specifically notes you should clarify whether they want top 3 rows or top 3 distinct values.

## Example

```sql
with ranked as (
  select
    department,
    employee_id,
    salary,
    dense_rank() over (
      partition by department
      order by salary desc
    ) as rnk
  from employees
)
select *
from ranked
where rnk <= 3;
```

## How to think about it

First ask:

* Do they want top 3 **rows**?
* Or top 3 **salary levels**?

That decides the function.

---

# 6) Pattern 3 - Change detection with `LAG`

This is one of the most reusable patterns in SQL.

Your roadmap explicitly includes `LAG / LEAD - change detection + session start patterns`. 

## Problem shape

“Tell me when status changed.”
“Find rows where price changed.”
“Mark first event after inactivity.”
“Compare today to yesterday.”

## Example

Table: `order_status_history`

| order_id | status_time | status  |
| -------- | ----------- | ------- |
| 101      | 09:00       | placed  |
| 101      | 09:05       | placed  |
| 101      | 09:10       | packed  |
| 101      | 09:20       | shipped |

## Query

```sql
with x as (
  select
    order_id,
    status_time,
    status,
    lag(status) over (
      partition by order_id
      order by status_time
    ) as prev_status
  from order_status_history
)
select
  *,
  case
    when prev_status is null then 1
    when status <> prev_status then 1
    else 0
  end as is_change
from x;
```

## Output idea

| order_id | status_time | status  | prev_status | is_change |
| -------- | ----------- | ------- | ----------- | --------- |
| 101      | 09:00       | placed  | null        | 1         |
| 101      | 09:05       | placed  | placed      | 0         |
| 101      | 09:10       | packed  | placed      | 1         |
| 101      | 09:20       | shipped | packed      | 1         |

## How to think about it

Bring previous row next to current row.
Then compare.

That is all.

## Gotcha

If values can be null, `<>` is risky. In Postgres, prefer:

```sql
status is distinct from prev_status
```

---

# 7) Pattern 4 - Running totals and moving windows

Your roadmap explicitly marks `SUM/AVG OVER with ROWS BETWEEN` and says running total and moving average should be written from memory; it also calls out `ROWS BETWEEN vs RANGE BETWEEN`.

## Problem shape

“Cumulative revenue.”
“7-day moving average.”
“Running balance.”

## Example: running total

Table: `daily_sales`

| sale_date  | amount |
| ---------- | ------ |
| 2025-01-01 | 100    |
| 2025-01-02 | 80     |
| 2025-01-03 | 50     |

```sql
select
  sale_date,
  amount,
  sum(amount) over (
    order by sale_date
    rows between unbounded preceding and current row
  ) as running_total
from daily_sales;
```

## Output

| sale_date  | amount | running_total |
| ---------- | ------ | ------------- |
| 2025-01-01 | 100    | 100           |
| 2025-01-02 | 80     | 180           |
| 2025-01-03 | 50     | 230           |

## Example: 3-row moving average

```sql
select
  sale_date,
  amount,
  avg(amount) over (
    order by sale_date
    rows between 2 preceding and current row
  ) as moving_avg_3
from daily_sales;
```

## Big gotcha: `ROWS` vs `RANGE`

Use `ROWS` when you want row-count-based behavior.
`RANGE` can include tied rows with same ordering value and surprise you.

For interviews and most operational SQL, default to `ROWS` unless you specifically need `RANGE`.

---

# 8) Pattern 5 - Gaps and islands

This is a classic “looks impossible until you know the trick” problem.

Your roadmap explicitly calls out gaps and islands, including the `ROW_NUMBER subtraction trick`, and longest streak per group.

## What it means

* **Gap** = missing period in a sequence
* **Island** = consecutive run of rows

Examples:

* consecutive login days
* uninterrupted device uptime
* continuous stock trading streak
* attendance streak

## The standard row_number subtraction trick

Suppose you want consecutive active days per user.

Table: `logins`

| user_id | login_date |
| ------- | ---------- |
| 1       | 2025-01-01 |
| 1       | 2025-01-02 |
| 1       | 2025-01-03 |
| 1       | 2025-01-05 |
| 1       | 2025-01-06 |

## Query

```sql
with x as (
  select
    user_id,
    login_date,
    row_number() over (
      partition by user_id
      order by login_date
    ) as rn
  from logins
),
y as (
  select
    user_id,
    login_date,
    login_date - (rn * interval '1 day') as grp
  from x
)
select
  user_id,
  min(login_date) as streak_start,
  max(login_date) as streak_end,
  count(*) as streak_len
from y
group by user_id, grp
order by user_id, streak_start;
```

## Why it works

For consecutive dates, the difference:

`login_date - rn * 1 day`

stays constant.

That constant becomes the island label.

Your interview guide even gives the one-line memory aid:
`grp = date - ROW_NUMBER() OVER (PARTITION BY user ORDER BY date)`. 

## Output idea

| user_id | streak_start | streak_end | streak_len |
| ------- | ------------ | ---------- | ---------- |
| 1       | 2025-01-01   | 2025-01-03 | 3          |
| 1       | 2025-01-05   | 2025-01-06 | 2          |

## How to think about it

Turn ordered rows into a fake “group id” that remains stable during consecutive runs.

---

# 9) Pattern 6 - Longest streak per group

This is just a layer on top of gaps and islands.

```sql
with x as (
  select
    user_id,
    login_date,
    row_number() over (
      partition by user_id
      order by login_date
    ) as rn
  from logins
),
y as (
  select
    user_id,
    login_date,
    login_date - (rn * interval '1 day') as grp
  from x
),
streaks as (
  select
    user_id,
    min(login_date) as streak_start,
    max(login_date) as streak_end,
    count(*) as streak_len
  from y
  group by user_id, grp
),
ranked as (
  select
    *,
    row_number() over (
      partition by user_id
      order by streak_len desc, streak_end desc
    ) as rn
  from streaks
)
select *
from ranked
where rn = 1;
```

This is a perfect example of combining patterns:

* row_number
* island labeling
* aggregation
* ranking again

---

# 10) Pattern 7 - Sessionization

Your roadmap explicitly lists sessionization and says: assign session ID using `LAG + conditional SUM window`.

This is one of the most important DE analytics patterns.

## Problem shape

“Group user events into sessions when inactivity > 30 min.”

## Example

Table: `events`

| user_id | event_time |
| ------- | ---------- |
| 1       | 10:00      |
| 1       | 10:05      |
| 1       | 10:50      |
| 1       | 11:00      |
| 1       | 12:10      |

Threshold = 30 minutes.

## Query

```sql
with x as (
  select
    user_id,
    event_time,
    lag(event_time) over (
      partition by user_id
      order by event_time
    ) as prev_event_time
  from events
),
y as (
  select
    *,
    case
      when prev_event_time is null then 1
      when event_time - prev_event_time > interval '30 minutes' then 1
      else 0
    end as is_session_start
  from x
)
select
  *,
  sum(is_session_start) over (
    partition by user_id
    order by event_time
    rows between unbounded preceding and current row
  ) as session_id
from y;
```

## Output idea

| user_id | event_time | is_session_start | session_id |
| ------- | ---------- | ---------------- | ---------- |
| 1       | 10:00      | 1                | 1          |
| 1       | 10:05      | 0                | 1          |
| 1       | 10:50      | 1                | 2          |
| 1       | 11:00      | 0                | 2          |
| 1       | 12:10      | 1                | 3          |

## How to think about it

Two steps:

* detect boundaries
* cumulative-sum boundaries into group numbers

That is a huge pattern in SQL.

---

# 11) Pattern 8 - Anti-join

Your roadmap explicitly says to know `NOT EXISTS` vs `LEFT JOIN + IS NULL`, and your interview guide includes anti-join as a memory item.

## Problem shape

“Find customers with no orders.”
“Find source records not yet loaded.”
“Find employees not mapped to a department.”

## Example

Tables:

`customers`

| customer_id |
| ----------- |
| 1           |
| 2           |
| 3           |

`orders`

| order_id | customer_id |
| -------- | ----------- |
| 10       | 1           |
| 11       | 1           |
| 12       | 2           |

## Query with `NOT EXISTS`

```sql
select c.customer_id
from customers c
where not exists (
  select 1
  from orders o
  where o.customer_id = c.customer_id
);
```

## Output

`3`

## Alternative

```sql
select c.customer_id
from customers c
left join orders o
  on c.customer_id = o.customer_id
where o.customer_id is null;
```

## How to think about it

This is not “joining data together.”
It is “asking whether a matching row exists.”

## Which is better?

Conceptually, `NOT EXISTS` expresses intent more clearly and often behaves better semantically, especially around nulls.

---

# 12) Pattern 9 - Join explosion / fan-out detection

This is a very real data engineering problem, and your roadmap explicitly tells you to learn to detect fan-out and pre-dedup before joining. 

## Problem shape

“You joined two tables and row counts exploded.”
“Revenue doubled after a join.”
“Why is my metric inflated?”

## Why it happens

You assumed one-to-one.
Reality was one-to-many or many-to-many.

## Example

`orders`

| order_id | customer_id |
| -------- | ----------- |
| 100      | 1           |
| 101      | 1           |

`order_items`

| order_id | product_id |
| -------- | ---------- |
| 100      | A          |
| 100      | B          |
| 101      | C          |

If you join `orders` to `order_items`, order 100 becomes 2 rows.

That is expected.

But if you then join to another multi-row table, the multiplication gets worse.

## Detection pattern

Before join, check uniqueness:

```sql
select customer_id, count(*)
from customers
group by customer_id
having count(*) > 1;
```

```sql
select order_id, count(*)
from order_items
group by order_id
having count(*) > 1;
```

## Safe fix pattern

Pre-aggregate or pre-dedup before joining.

Example: one row per order first

```sql
with item_counts as (
  select
    order_id,
    count(*) as item_count
  from order_items
  group by order_id
)
select
  o.order_id,
  o.customer_id,
  i.item_count
from orders o
left join item_counts i
  on o.order_id = i.order_id;
```

## How to think about it

Every join has a cardinality contract:

* one-to-one
* one-to-many
* many-to-one
* many-to-many

Never join blindly.

---

# 13) Pattern 10 - Set operations: `EXCEPT`, `INTERSECT`

Your roadmap explicitly lists set operations and says understand their use in SCD2 change detection. 

## Problem shape

“What rows exist in source but not in target?”
“What rows are common between both snapshots?”
“What changed between two extracts?”

## Example

`source_snapshot`

| customer_id | city   |
| ----------- | ------ |
| 1           | Delhi  |
| 2           | Pune   |
| 3           | Mumbai |

`target_snapshot`

| customer_id | city    |
| ----------- | ------- |
| 1           | Delhi   |
| 2           | Chennai |

## New or changed rows in source:

```sql
select customer_id, city
from source_snapshot

except

select customer_id, city
from target_snapshot;
```

## Output

| customer_id | city   |
| ----------- | ------ |
| 2           | Pune   |
| 3           | Mumbai |

## How to think about it

This is row-set comparison, not row-by-row procedural logic.

Very elegant when schemas align.

---

# 14) Pattern 11 - SCD2-style change detection

Your roadmap and interview guide both call out SCD2 as important.

## Problem shape

“Preserve history when tracked attributes change.”

## Table idea

Current dimension:

| customer_key | customer_id | city  | valid_from | valid_to   | is_current |
| ------------ | ----------- | ----- | ---------- | ---------- | ---------- |
| 10           | 1           | Delhi | 2025-01-01 | 9999-12-31 | 1          |

New source snapshot:

| customer_id | city   |
| ----------- | ------ |
| 1           | Mumbai |

## What should happen

* old row closed: valid_to = yesterday, is_current = 0
* new row inserted: city = Mumbai, valid_from = today, is_current = 1

## Change detection options

* join source to current target and compare columns
* use hashdiff
* use `EXCEPT`

Example detection join:

```sql
select
  s.customer_id,
  s.city as new_city,
  d.city as old_city
from source_customers s
join dim_customers d
  on s.customer_id = d.customer_id
 and d.is_current = 1
where s.city is distinct from d.city;
```

## How to think about it

This is just change detection plus history management.

---

# 15) Pattern combinations - where the hard problems really come from

The nastiest SQL questions often combine 2 or 3 patterns.

Examples:

## A. Longest paid subscription streak per user

Needs:

* gaps and islands
* filtering
* ranking

## B. Latest active plan per customer excluding canceled plans

Needs:

* filtering
* dedup latest row per key
* maybe anti-join if cancellations are separate

## C. Count sessions per user per day

Needs:

* sessionization
* date extraction
* aggregation

## D. Find products never sold after launch

Needs:

* anti-join
* time filter

## E. Find top 3 longest inactivity gaps per customer

Needs:

* `LAG/LEAD`
* interval calculation
* ranking

That is why “advanced SQL” feels hard: it is usually not one operator. It is **pattern composition**.

---

# 16) Real work examples from data engineering

This is not just for interviews.

## Dedup latest row per key

Used in:

* CDC ingestion
* bronze → silver cleanup
* latest status snapshot tables

## Anti-join

Used in:

* incremental loads
* identifying missing records
* reconciliation

## Sessionization

Used in:

* clickstream analytics
* user behavior pipelines
* activity grouping

## Gaps and islands

Used in:

* uptime streaks
* attendance streaks
* consecutive transaction days

## Set difference

Used in:

* source vs target validation
* data quality checks
* SCD2 change detection

## Fan-out detection

Used in:

* debugging inflated metrics
* preventing broken marts
* safe pipeline design

This is why your roadmap pushes these so aggressively.

---

# 17) Common mistakes and gotchas

These are the things that cause even good SQL users to fail.

## 1. Not defining the grain

If one row means “order item” but you think it means “order,” your query lies.

## 2. Missing deterministic ordering

Window functions need stable ordering.

Bad:

```sql
order by updated_at desc
```

Better:

```sql
order by updated_at desc, load_id desc
```

## 3. Filtering too early

If you filter rows before `LAG`, `LEAD`, ranking, or sessionization, you might destroy the sequence.

## 4. Wrong ranking function

`ROW_NUMBER` vs `RANK` vs `DENSE_RANK` is not cosmetic.

## 5. Ignoring null semantics

`NULL <> NULL` is not true.
Use `IS DISTINCT FROM` where available.

## 6. Using `LEFT JOIN` without thinking about multiplicity

Metrics inflate silently.

## 7. Forgetting business definition

“Top 3 salaries” is ambiguous.
“Latest row” may need tie handling.
“Session” needs a threshold definition.

## 8. Using `RANGE` accidentally

Can behave unexpectedly on tied order values.

---

# 18) How to attack a tricky SQL problem in interviews

Here is the approach I want you to internalize.

## Step 1: Restate the grain

“Does one row represent an event, an order, or a customer snapshot?”

## Step 2: Clarify output

“Do you want one row per customer, per session, or per day?”

## Step 3: Clarify ties and duplicates

“If two rows have the same timestamp, how should I break the tie?”

## Step 4: Identify the pattern

Ask yourself:

* dedup?
* ranking?
* previous row comparison?
* sequence grouping?
* anti-join?
* set difference?

## Step 5: Build in stages with CTEs

Do not try to be clever in one giant query.

Good interview SQL is often:

* CTE 1 = annotate rows
* CTE 2 = derive flags
* CTE 3 = aggregate/rank/filter

That is how readable production SQL is written too.

---

# 19) A full multi-pattern example

Let’s solve something slightly more realistic.

## Problem

For each user, find their longest streak of consecutive login days.

## Table

`user_logins`

| user_id | login_date |
| ------- | ---------- |
| 1       | 2025-01-01 |
| 1       | 2025-01-02 |
| 1       | 2025-01-03 |
| 1       | 2025-01-05 |
| 1       | 2025-01-06 |
| 2       | 2025-02-01 |
| 2       | 2025-02-03 |

## Query

```sql
with deduped as (
  select distinct
    user_id,
    login_date
  from user_logins
),
numbered as (
  select
    user_id,
    login_date,
    row_number() over (
      partition by user_id
      order by login_date
    ) as rn
  from deduped
),
islands as (
  select
    user_id,
    login_date,
    login_date - (rn * interval '1 day') as grp
  from numbered
),
streaks as (
  select
    user_id,
    min(login_date) as streak_start,
    max(login_date) as streak_end,
    count(*) as streak_len
  from islands
  group by user_id, grp
),
ranked as (
  select
    *,
    row_number() over (
      partition by user_id
      order by streak_len desc, streak_end desc
    ) as rn
  from streaks
)
select
  user_id,
  streak_start,
  streak_end,
  streak_len
from ranked
where rn = 1
order by user_id;
```

## What patterns did we use?

* dedup
* row numbering
* gaps/islands grouping
* aggregation
* ranking

That is advanced SQL.

---

# 20) Another full multi-pattern example

## Problem

Assign 30-minute sessions to user events and compute session summaries.

Table: `events(user_id, event_time, event_name)`

```sql
with x as (
  select
    user_id,
    event_time,
    event_name,
    lag(event_time) over (
      partition by user_id
      order by event_time
    ) as prev_event_time
  from events
),
y as (
  select
    *,
    case
      when prev_event_time is null then 1
      when event_time - prev_event_time > interval '30 minutes' then 1
      else 0
    end as is_session_start
  from x
),
z as (
  select
    *,
    sum(is_session_start) over (
      partition by user_id
      order by event_time
      rows between unbounded preceding and current row
    ) as session_id
  from y
)
select
  user_id,
  session_id,
  min(event_time) as session_start,
  max(event_time) as session_end,
  count(*) as event_count
from z
group by user_id, session_id
order by user_id, session_id;
```

## What patterns are combined?

* lag
* change boundary detection
* cumulative sum
* aggregation

---

# 21) What to memorize vs what to understand

## Must memorize

These should be nearly muscle memory for you:

### Latest row per key

```sql
row_number() over (
  partition by key
  order by ts desc
)
```

### Top N per group

```sql
dense_rank() over (
  partition by grp
  order by metric desc
)
```

### Previous-row comparison

```sql
lag(col) over (
  partition by key
  order by ts
)
```

### Running total

```sql
sum(val) over (
  partition by key
  order by ts
  rows between unbounded preceding and current row
)
```

### Sessionization idea

```sql
lag(ts) -> gap flag -> cumulative sum
```

### Gaps and islands idea

```sql
date - row_number()
```

### Anti-join

```sql
where not exists (...)
```

### Snowflake latest-row shortcut

```sql
qualify row_number() over (...) = 1
```

These exact areas are aligned with your roadmap and interview guide.

## Must understand deeply

These you should understand, not just recite:

* grain
* cardinality of joins
* tie handling
* null semantics
* why a pattern works
* when to pre-aggregate before joining
* when a set operation is cleaner than a join
* how to build a solution in layered CTEs

---

# 22) Interview-quality verbal answer

Here is a strong answer you can actually say:

> Advanced SQL problems usually reduce to a few reusable patterns. I first identify the grain of the data, the grouping key, and whether row order matters. Then I map the problem to a pattern such as deduplication with `ROW_NUMBER`, change detection with `LAG`, sessionization with `LAG` plus cumulative `SUM`, gaps-and-islands with a row-number grouping trick, or anti-joins with `NOT EXISTS`. In real data engineering work, these patterns show up in CDC deduping, session analytics, incremental loads, reconciliation, and SCD2 history tracking. My approach is to solve them in stages with CTEs so the logic stays correct and explainable.

That is a very good 15–18 LPA SQL answer.

---

# 23) Practice problems for you

Do these in order.

## 1. Easy-medium

Given `customer_orders(customer_id, order_id, order_time)`, return the latest order per customer.

## 2. Easy-medium

Given `employees(department, employee_id, salary)`, return the top 3 distinct salary values per department.

## 3. Medium

Given `product_prices(product_id, price_date, price)`, return rows where the price changed from the previous row.

## 4. Medium

Given `daily_sales(sale_date, amount)`, compute:

* running total
* 7-row moving average

## 5. Medium-hard

Given `user_logins(user_id, login_date)`, return all consecutive login streaks per user.

## 6. Hard

From the same login table, return only the longest streak per user.

## 7. Hard

Given `events(user_id, event_time)`, assign session IDs where inactivity > 30 minutes.

## 8. Hard

Given `customers` and `orders`, return customers with no orders using:

* `NOT EXISTS`
* `LEFT JOIN ... IS NULL`

Then explain which you prefer and why.

## 9. Hard

Given `orders`, `order_items`, and `item_discounts`, show how a naive join can inflate totals, then fix it with pre-aggregation.

## 10. Hard

Given `source_snapshot` and `target_current`, detect changed rows for SCD2 processing using either join-based comparison or `EXCEPT`.

---

# 24) The best way for you to study this topic

Since your roadmap already pushes daily window-function practice, here is the best study loop for this topic:

Day 1:

* latest-row dedup
* top N per group

Day 2:

* `LAG / LEAD`
* change detection

Day 3:

* running totals
* moving windows
* `ROWS` vs `RANGE`

Day 4:

* gaps and islands

Day 5:

* sessionization

Day 6:

* anti-joins
* fan-out detection
* set operations

Day 7:

* mixed problems combining 2–3 patterns

That sequence matches the structure of your roadmap very well.

---

# 25) Final takeaway

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

Once you can recognize those shapes, “tricky” SQL stops feeling magical.

For your transition into data engineering, this is high ROI because your roadmap and guide both make clear that SQL is tested everywhere, and that these exact window-function and hard analytics patterns are central to success in your target band.