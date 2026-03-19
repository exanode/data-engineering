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


--- 

# Advanced SQL: the Missing Pieces 

## What these missing topics really are

These are not random extras.

They are the pieces that make an interviewer feel:

> “This person does not just know SQL syntax. They understand how SQL behaves, why queries go wrong, and how to write production-safe SQL.”

That is the jump from:

* “can solve SQL problems”
  to
* “can own warehouse logic and debug pipelines”

These missing areas mostly fall into 5 buckets:

1. **How SQL actually executes**
2. **How to choose the right pattern**
3. **How NULLs and joins silently break correctness**
4. **How large-table SQL behaves in real data systems**
5. **How to think like a data engineer, not just a query writer**

---

# 1) SQL Execution Order + Why `QUALIFY` Exists

## Intuition

A lot of SQL confusion comes from this:

You write SQL **top to bottom**, but the database does **not execute it top to bottom**.

This matters because many interview traps are really about execution order:

* “Why can’t I use `ROW_NUMBER()` in `WHERE`?”
* “Why do I need a subquery?”
* “Why does `QUALIFY` help?”

If you deeply understand execution order, many advanced SQL problems become much easier.

---

## The practical execution order

A useful mental model is:

```sql
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

Different engines may describe internal planning differently, but for interview and writing purposes, this order is the right mental model.

### Key idea

Window functions are computed **after** `WHERE` and `GROUP BY`, but **before final ordering/output**.

That is why this fails:

```sql
SELECT
  customer_id,
  order_id,
  ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY order_time DESC
  ) AS rn
FROM orders
WHERE rn = 1;
```

Because at the `WHERE` stage, `rn` does not exist yet.

---

## The classic fix: subquery / CTE

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
SELECT *
FROM ranked
WHERE rn = 1;
```

### How to think about it

Break the problem into stages:

1. build rows
2. compute row numbers
3. filter on row numbers

This “layered CTE thinking” is one of the most important advanced SQL habits.

---

## `QUALIFY`: the modern shortcut

Some databases like Snowflake and BigQuery support `QUALIFY`, which lets you filter **after window functions** without needing a subquery. Your roadmap explicitly calls out `QUALIFY` in Snowflake. 

```sql
SELECT
  customer_id,
  order_id,
  order_time
FROM orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY order_time DESC
) = 1;
```

### Why it exists

Because filtering windowed results is such a common pattern that subqueries become noisy.

### Where it is used

* latest row per key
* top N per group
* dedup
* first event after condition
* keeping one row after ranking

---

## Interview angle

### Typical question

“Return the latest order per customer.”

### Good answer

* portable SQL: use CTE/subquery + `ROW_NUMBER`
* Snowflake/BigQuery: use `QUALIFY`

### What interviewer is testing

Not just syntax. They are testing whether you understand:

* ranking
* ordering
* filtering stage
* dialect-aware simplification

---

## Example with sample table

### `orders`

| customer_id | order_id | order_time          |
| ----------- | -------- | ------------------- |
| 1           | 101      | 2025-01-01 10:00:00 |
| 1           | 102      | 2025-01-03 09:00:00 |
| 2           | 201      | 2025-01-02 11:00:00 |
| 2           | 202      | 2025-01-02 12:00:00 |

### Query

```sql
SELECT
  customer_id,
  order_id,
  order_time
FROM orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY order_time DESC
) = 1;
```

### Output

| customer_id | order_id | order_time          |
| ----------- | -------- | ------------------- |
| 1           | 102      | 2025-01-03 09:00:00 |
| 2           | 202      | 2025-01-02 12:00:00 |

---

## Common mistakes

### Mistake 1: trying to use window output in `WHERE`

Wrong mental model.

### Mistake 2: forgetting tie-breakers

If two rows have same timestamp, result may be nondeterministic.

Better:

```sql
ORDER BY order_time DESC, order_id DESC
```

### Mistake 3: assuming `QUALIFY` is universal

It is not portable to all engines.

---

## Reusable mental model

When you see:

* latest row
* first row
* top N per group
* dedup

Think:

> “I need to rank first, then filter after ranking.”

If dialect supports `QUALIFY`, use it.
Otherwise, use a CTE/subquery.

---

# 2) Choosing Between `DISTINCT`, `GROUP BY`, and Window Functions

## Intuition

Many people know all three, but do not know **which one matches the problem shape**.

This is a very common interview differentiator.

Because often the wrong SQL still runs — but it solves the wrong problem.

---

## The core difference

### `DISTINCT`

Use when you just want to remove duplicate rows from the final output.

### `GROUP BY`

Use when you want to **collapse rows into fewer rows** and aggregate.

### Window functions

Use when you want to **keep original rows** but compute group-level or sequence-level information.

That is the whole decision.

---

## Mental model

Ask:

### 1. Do I want fewer rows?

Use `GROUP BY`.

### 2. Do I want the same number of rows, plus extra metrics?

Use window functions.

### 3. Do I just want duplicate output rows removed, without business logic?

Use `DISTINCT`.

---

## Example 1: `DISTINCT`

### Table `events`

| user_id | page |
| ------- | ---- |
| 1       | home |
| 1       | home |
| 1       | cart |
| 2       | home |

```sql
SELECT DISTINCT user_id, page
FROM events;
```

### Output

| user_id | page |
| ------- | ---- |
| 1       | home |
| 1       | cart |
| 2       | home |

### What happened

You did not compute anything.
You only removed duplicate output rows.

---

## Example 2: `GROUP BY`

Suppose you want page counts per user.

```sql
SELECT
  user_id,
  COUNT(*) AS page_views
FROM events
GROUP BY user_id;
```

### Output

| user_id | page_views |
| ------- | ---------- |
| 1       | 3          |
| 2       | 1          |

### What happened

You collapsed multiple event rows into one row per user.

---

## Example 3: window function

Suppose you want each row plus total page views for that user.

```sql
SELECT
  user_id,
  page,
  COUNT(*) OVER (PARTITION BY user_id) AS user_page_views
FROM events;
```

### Output

| user_id | page | user_page_views |
| ------- | ---- | --------------- |
| 1       | home | 3               |
| 1       | home | 3               |
| 1       | cart | 3               |
| 2       | home | 1               |

### What happened

You kept all rows, but attached group-level info to each row.

---

## Common interview trap

“Why not use `GROUP BY` here?”

### Example

Return each employee row along with department average salary.

Bad approach:

```sql
SELECT dept, AVG(salary)
FROM employees
GROUP BY dept;
```

This loses employee rows.

Correct:

```sql
SELECT
  emp_id,
  dept,
  salary,
  AVG(salary) OVER (PARTITION BY dept) AS dept_avg_salary
FROM employees;
```

---

## Real-world DE use cases

### `DISTINCT`

* cleaning obvious duplicate outputs
* deduping staging data only in simple cases

### `GROUP BY`

* building daily aggregates
* fact table summarization
* KPI reporting

### window functions

* customer-level stats on event rows
* ranking latest records
* running totals
* comparing rows across time

---

## Common mistakes

### Mistake 1: using `DISTINCT` to hide join problems

This is dangerous.

People often do:

```sql
SELECT DISTINCT ...
FROM a
JOIN b ...
```

because duplicates appeared after a bad join.

That is not a fix. That is hiding a grain/cardinality bug.

### Mistake 2: using `GROUP BY` when row preservation is needed

You lose row-level detail.

### Mistake 3: using windows when aggregation is enough

Sometimes you actually want a summary table, not row-level output.

---

## Reusable mental model

Ask this exact question in your head:

> “Should the result have fewer rows, same rows, or just unique rows?”

* fewer rows → `GROUP BY`
* same rows → window
* unique rows only → `DISTINCT`

---

# 3) NULL Semantics: the Silent Source of Wrong SQL

## Intuition

NULL is not a value like 0 or empty string.

NULL means:

> “unknown” or “missing”

Because of that, SQL comparisons involving NULL behave differently than most people expect.

This is one of the highest-ROI topics for interviews because many “mystery bugs” are really NULL bugs.

Your notes mention null semantics, but for interview readiness this topic deserves its own deep treatment. 

---

## The first rule: `NULL = NULL` is not true

It is not true.
It is not false.
It is unknown.

So this does not work:

```sql
WHERE col = NULL
```

Correct:

```sql
WHERE col IS NULL
```

And:

```sql
WHERE col IS NOT NULL
```

---

## Three-valued logic

SQL uses:

* TRUE
* FALSE
* UNKNOWN

Rows pass a `WHERE` filter only when condition is TRUE.

So if a condition becomes UNKNOWN, the row is filtered out.

That explains a lot of weird behavior.

---

## Example

### Table `users`

| user_id | country |
| ------- | ------- |
| 1       | IN      |
| 2       | NULL    |
| 3       | US      |

### Query

```sql
SELECT *
FROM users
WHERE country <> 'IN';
```

You might expect rows 2 and 3.
Actual result: only row 3.

Why?

* row 1: `'IN' <> 'IN'` → FALSE
* row 2: `NULL <> 'IN'` → UNKNOWN
* row 3: `'US' <> 'IN'` → TRUE

Only TRUE survives.

---

## `COUNT(*)` vs `COUNT(col)`

This is a classic interview question.

### Table `payments`

| payment_id | amount |
| ---------- | ------ |
| 1          | 100    |
| 2          | NULL   |
| 3          | 50     |

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(amount) AS non_null_amount_rows
FROM payments;
```

### Output

| total_rows | non_null_amount_rows |
| ---------- | -------------------- |
| 3          | 2                    |

### Why

* `COUNT(*)` counts rows
* `COUNT(col)` counts non-null values in that column

---

## `SUM`, `AVG`, `MIN`, `MAX`

These ignore NULLs in most SQL engines.

```sql
SELECT AVG(amount)
FROM payments;
```

This averages only 100 and 50, not the NULL row.

---

## Safer comparison: `IS DISTINCT FROM`

In some engines like Postgres, this is extremely useful.

```sql
SELECT *
FROM t
WHERE col1 IS DISTINCT FROM col2;
```

This treats NULLs more intuitively for change detection:

* NULL vs NULL → not distinct
* NULL vs 5 → distinct
* 5 vs 5 → not distinct

This is much safer than `<>` in many DE scenarios.

Your notes already hint at this for `LAG` comparisons, but you should generalize the idea. 

---

## Real-world DE use cases

* CDC comparison between source and target
* null-safe change detection
* reconciliation queries
* quality checks on required fields
* join debugging

---

## Common mistakes

### Mistake 1: `= NULL`

Always wrong.

### Mistake 2: assuming `NULL <> value` is TRUE

It is UNKNOWN.

### Mistake 3: forgetting NULL behavior inside joins

More on that next.

---

## Reusable mental model

Whenever NULL is involved, ask:

> “Is this value missing? Then comparison may become UNKNOWN.”

And whenever comparing fields for equality/inequality in change detection, ask:

> “Do I need null-safe comparison?”

---

# 4) Anti-Joins Properly: `NOT EXISTS` vs `LEFT JOIN ... IS NULL` vs `NOT IN`

## Intuition

An anti-join means:

> “Give me rows from A that do **not** have a match in B.”

This pattern shows up constantly in data engineering:

* records in source not yet loaded
* customers with no orders
* target rows no longer present in source
* missing mappings
* reconciliation failures

Your notes cover anti-join at the pattern level. What was still missing was the **deep correctness discussion**, especially `NOT IN` and NULLs. 

---

## Pattern 1: `NOT EXISTS` — safest mental model

```sql
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.customer_id
);
```

### How to think about it

For each customer, ask:
“Does at least one matching order exist?”
If no, keep the customer.

This expresses intent very clearly.

---

## Pattern 2: `LEFT JOIN ... IS NULL`

```sql
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o
  ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

### How to think about it

Left join keeps all customers.
If no matching order exists, order columns are NULL.
So filtering `o.customer_id IS NULL` keeps only unmatched customers.

---

## Pattern 3: `NOT IN` — dangerous when NULLs exist

```sql
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (
  SELECT customer_id
  FROM orders
);
```

This looks fine.
But if `orders.customer_id` contains even one NULL, the logic can break.

---

## Why `NOT IN` breaks

Suppose `orders.customer_id` is:

| customer_id |
| ----------- |
| 1           |
| 2           |
| NULL        |

Now SQL effectively evaluates:

```sql
customer_id NOT IN (1, 2, NULL)
```

For customer 3, that becomes something like:

* 3 <> 1 → TRUE
* 3 <> 2 → TRUE
* 3 <> NULL → UNKNOWN

Combined result becomes UNKNOWN, so row is filtered out.

This can make the query return **no rows**, which shocks many candidates.

---

## Safe version if you must use `NOT IN`

```sql
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (
  SELECT customer_id
  FROM orders
  WHERE customer_id IS NOT NULL
);
```

But in interviews, `NOT EXISTS` is usually the better answer.

---

## Which one should you prefer?

### Prefer `NOT EXISTS` when:

* you want the cleanest semantic meaning
* nullable keys exist
* you want safer interview-grade correctness

### Use `LEFT JOIN ... IS NULL` when:

* it reads naturally in a broader join pipeline
* you also need columns from the left side in a join-style query

### Be careful with `NOT IN`

Only use when you are very sure the subquery column cannot contain NULLs.

---

## Step-by-step example

### `customers`

| customer_id |
| ----------- |
| 1           |
| 2           |
| 3           |
| 4           |

### `orders`

| order_id | customer_id |
| -------- | ----------- |
| 101      | 1           |
| 102      | 1           |
| 103      | 2           |

### Query

```sql
SELECT c.customer_id
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.customer_id
);
```

### Output

| customer_id |
| ----------- |
| 3           |
| 4           |

---

## Interview question

“Find employees not assigned to a department.”

### Strong verbal answer

“I would usually write this as `NOT EXISTS` because anti-join is really an existence check, and `NOT EXISTS` is robust even when nullable data is involved. `LEFT JOIN ... IS NULL` is also valid, but I avoid `NOT IN` unless I know the subquery cannot return NULLs.”

---

# 5) Correlated Subqueries vs Window Functions

## Intuition

A correlated subquery is a subquery that depends on the current row of the outer query.

It is not always wrong.
But many advanced SQL problems that juniors solve with correlated subqueries are cleaner and more scalable with window functions.

Interviewers love this comparison because it reveals whether you think in **row-by-row logic** or **set-based logic**.

---

## Correlated subquery example

### Problem

Find employees whose salary is above their department average.

### Query

```sql
SELECT
  e.emp_id,
  e.dept,
  e.salary
FROM employees e
WHERE e.salary > (
  SELECT AVG(e2.salary)
  FROM employees e2
  WHERE e2.dept = e.dept
);
```

### Why it works

For each employee row, the subquery recomputes average salary for that row’s department.

Conceptually correct.

---

## Window version

```sql
WITH x AS (
  SELECT
    emp_id,
    dept,
    salary,
    AVG(salary) OVER (PARTITION BY dept) AS dept_avg_salary
  FROM employees
)
SELECT
  emp_id,
  dept,
  salary
FROM x
WHERE salary > dept_avg_salary;
```

### Why this is often better

You compute department average once as a window metric on the rowset, then filter.

This is often:

* clearer
* more extensible
* better for layered analytics

---

## How to think about it

### Correlated subquery mindset

“For this row, look up something about related rows.”

### Window mindset

“Bring group-level context onto each row, then compare.”

For analytic SQL, window thinking is usually stronger.

---

## Another example: latest row per user

A correlated-subquery way:

```sql
SELECT o.*
FROM orders o
WHERE o.order_time = (
  SELECT MAX(o2.order_time)
  FROM orders o2
  WHERE o2.customer_id = o.customer_id
);
```

This may return multiple rows on ties.

Window version:

```sql
SELECT *
FROM orders
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY customer_id
  ORDER BY order_time DESC, order_id DESC
) = 1;
```

This gives deterministic one-row-per-customer logic.

---

## Real-world DE use cases

* latest status per entity
* comparing values to group avg/max/min
* identifying first/last events
* ranking rows for dedup and CDC cleanup

---

## Common mistakes

### Mistake 1: using correlated subquery when ties matter

You may get multiple rows unintentionally.

### Mistake 2: thinking correlated subquery is always bad

It is not. Sometimes it is readable and optimizer handles it well.

### Mistake 3: not recognizing window functions as the analytic upgrade

For grouped row-wise comparisons, windows are often the right abstraction.

---

## Reusable mental model

If the problem says:

* compare row to group metric
* rank within group
* attach prior/next/group context to each row

Think:

> “This smells like a window function.”

---

# 6) Join Explosion, Cardinality, and Grain Control

## Intuition

This is one of the most important real-world DE skills.

Many SQL answers are “correct” syntactically but wrong analytically because joins changed the grain of the data.

This is what causes:

* inflated revenue
* duplicate users
* wrong counts
* broken dashboards
* expensive queries

Your notes cover fan-out detection, but what was still missing was a deeper **cardinality mental model**.  

---

## The most important question before a join

Ask:

> “What is the grain of each table?”
> and
> “What cardinality will this join create?”

---

## Cardinality types

### one-to-one

One row in A matches one row in B.

### one-to-many

One row in A matches many rows in B.

### many-to-one

Many rows in A match one row in B.

### many-to-many

Many rows in A match many rows in B.

This is the danger zone.

---

## Example: join explosion

### `orders`

| order_id | customer_id | order_amount |
| -------- | ----------- | ------------ |
| 100      | 1           | 500          |
| 101      | 1           | 200          |

### `order_items`

| order_id | item_id | item_price |
| -------- | ------- | ---------- |
| 100      | A       | 300        |
| 100      | B       | 200        |
| 101      | C       | 200        |

Now do:

```sql
SELECT
  o.order_id,
  o.order_amount,
  oi.item_id
FROM orders o
JOIN order_items oi
  ON o.order_id = oi.order_id;
```

### Output

| order_id | order_amount | item_id |
| -------- | ------------ | ------- |
| 100      | 500          | A       |
| 100      | 500          | B       |
| 101      | 200          | C       |

Notice `order_amount=500` is repeated twice.

If you now do:

```sql
SELECT SUM(o.order_amount)
FROM orders o
JOIN order_items oi
  ON o.order_id = oi.order_id;
```

You get:

* 500 + 500 + 200 = 1200

But real order revenue is:

* 500 + 200 = 700

This is a classic analytics bug.

---

## Correct ways to think

### If you need order-level totals

Do not join to item-level rows unless necessary.

### If you need item counts per order

Aggregate item table first:

```sql
WITH item_counts AS (
  SELECT
    order_id,
    COUNT(*) AS item_count
  FROM order_items
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.order_amount,
  i.item_count
FROM orders o
LEFT JOIN item_counts i
  ON o.order_id = i.order_id;
```

Now grain stays one row per order.

---

## Cardinality estimation habit

Before joining, ask:

* is the join key unique on left?
* is the join key unique on right?
* what will row count likely become?

### Example

* customers: 1M rows, unique by `customer_id`
* orders: 20M rows, many per customer

Then:

* customer → orders join will expand toward 20M rows, not stay 1M

This sounds obvious, but strong DEs always estimate row count mentally before running the join.

---

## Detection queries

### Check uniqueness

```sql
SELECT customer_id, COUNT(*)
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

```sql
SELECT order_id, COUNT(*)
FROM order_items
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### Check row counts before and after join

```sql
SELECT COUNT(*) FROM orders;

SELECT COUNT(*)
FROM orders o
JOIN order_items oi
  ON o.order_id = oi.order_id;
```

If row count jumps unexpectedly, inspect grain.

---

## Real-world DE use cases

* fact-to-fact joins
* attribution pipelines
* clickstream enrichment
* duplicate metric debugging
* dimensional model design
* dbt model validation

---

## Common mistakes

### Mistake 1: using `DISTINCT` to hide fan-out

This can silently corrupt business logic.

### Mistake 2: not defining output grain first

“one row per customer” vs “one row per customer order item” changes everything.

### Mistake 3: joining two fact-like tables directly

Often requires pre-aggregation or careful bridge logic.

---

## Reusable mental model

Before every join, say this to yourself:

> “What is the grain on the left, what is the grain on the right, and what will one joined row represent?”

If you cannot answer that clearly, do not trust the query yet.

---

# 7) Window Frames: Defaults, `ROWS` vs `RANGE`, and Why Ties Matter

## Intuition

Many people can write window functions, but fewer understand the frame.

This matters because:

* running totals may silently behave differently with duplicate order values
* interviewers may ask “what is the difference between `ROWS` and `RANGE`?”
* the default frame can surprise you

Your roadmap explicitly expects you to explain `ROWS BETWEEN` vs `RANGE BETWEEN`. 

---

## What is a frame?

Inside a window partition, the frame defines:

> “For this row, which neighboring rows are included in the calculation?”

For example:

* all prior rows up to current row
* current row only
* last 7 rows
* all rows with same order value

---

## The explicit `ROWS` running total

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

This means:
for each row, sum from first row up to current row by row position.

---

## Why default frame matters

If you write:

```sql
SUM(amount) OVER (ORDER BY sale_date)
```

many engines treat this like a default frame, often equivalent to:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

The exact behavior depends on engine and data type support, but interview-wise the key point is:

> default framing may not behave like explicit row-by-row accumulation when ties exist.

So for production clarity, prefer writing the frame explicitly.

---

## `ROWS` vs `RANGE`

### `ROWS`

Counts physical rows.

### `RANGE`

Groups rows by ordered value range, and tied values can be included together.

---

## Example with ties

### `sales`

| day | amount |
| --- | ------ |
| 1   | 100    |
| 1   | 50     |
| 2   | 30     |

Now compare:

### Query with `ROWS`

```sql
SELECT
  day,
  amount,
  SUM(amount) OVER (
    ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_rows
FROM sales;
```

### Output idea

| day | amount | running_rows |
| --- | ------ | ------------ |
| 1   | 100    | 100          |
| 1   | 50     | 150          |
| 2   | 30     | 180          |

Now with `RANGE`:

```sql
SELECT
  day,
  amount,
  SUM(amount) OVER (
    ORDER BY day
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_range
FROM sales;
```

### Output idea

For the first `day = 1` row, `RANGE` may include **all rows where day = 1**, so the result can jump straight to 150 even on the first tied row.

That is why `RANGE` can surprise you.

---

## Practical rule

For most interview and DE operational SQL:

> default to `ROWS`

Use `RANGE` only when you explicitly want value-based grouping behavior.

Your notes mention this briefly; what you needed was the deeper intuition about ties and default frames. 

---

## Common mistakes

### Mistake 1: omitting frame and assuming behavior

Be explicit.

### Mistake 2: using `RANGE` accidentally on tied order values

Running totals jump unexpectedly.

### Mistake 3: not having deterministic ordering

If timestamps tie, add a second tie-breaker when using row-based logic.

---

## Interview-quality explanation

“If I want running totals row by row, I prefer `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` because it behaves on physical row positions. `RANGE` is value-based and can pull in tied rows that share the same order key, so results can differ when duplicates exist.”

---

# 8) Incremental Processing Strategy: Full Refresh vs Incremental vs Idempotency

## Intuition

This is where SQL becomes data engineering.

A strong DE interview is not only:
“Can you write a query?”

It is also:
“How would you run this every day safely?”

Your notes cover `MERGE` and SCD2 mechanics, but the missing layer was the **strategy behind incremental processing**. 

---

## Why incremental processing exists

If a table has 5 billion rows, you do not want to recompute everything every day unless necessary.

So you often process:

* only new rows
* only changed rows
* only recent partitions

This reduces:

* runtime
* cost
* failure surface

---

## Three core strategies

### 1. Full refresh

Rebuild the entire target every run.

Good when:

* data is small
* logic is simple
* correctness matters more than cost
* early model stage

Bad when:

* data is huge
* SLA is tight

---

### 2. Incremental append

Load only newly arrived rows.

Example:

```sql
SELECT *
FROM source_events
WHERE event_time > (
  SELECT MAX(event_time)
  FROM target_events
);
```

This is simple but has edge cases.

---

### 3. Incremental upsert / merge

Insert new rows and update changed rows.

```sql
MERGE INTO target t
USING source_delta s
ON t.id = s.id
WHEN MATCHED AND t.hash_val <> s.hash_val THEN
  UPDATE SET ...
WHEN NOT MATCHED THEN
  INSERT (...);
```

This is common for dimensions, latest-state tables, and CDC-style pipelines.

---

## Watermarking

A watermark is the “how far have I processed?” marker.

Typical examples:

* max `updated_at`
* max `ingestion_id`
* max event date processed

### Example

```sql
SELECT *
FROM source_orders
WHERE updated_at > :last_watermark;
```

---

## Why watermarks are tricky

Suppose yesterday you processed up to:
`2025-01-10 10:00:00`

Today, a late record arrives with:
`2025-01-10 09:55:00`

If you strictly use `>` watermark, you miss it.

So many pipelines use a lookback window:

```sql
SELECT *
FROM source_orders
WHERE updated_at >= :last_watermark - INTERVAL '1 day';
```

Then deduplicate downstream.

---

## Idempotency

Idempotency means:

> running the same pipeline again should not create incorrect duplicates or inconsistent results.

This is a core DE principle.

### Example of non-idempotent load

```sql
INSERT INTO target_orders
SELECT *
FROM source_orders
WHERE order_date = CURRENT_DATE;
```

If the job reruns, same rows get inserted again.

### Better approach

* use `MERGE`
* use unique keys
* dedup in staging
* overwrite target partition safely

---

## Late-arriving data

This is extremely common in real systems:

* mobile devices sync late
* APIs retry late
* upstream batch arrives late

So incremental design must answer:

> “If a record arrives late, how do we still include it?”

Typical fixes:

* lookback windows
* event-time vs ingestion-time logic
* dedup by business key + latest timestamp
* periodic backfill

---

## Real-world DE use cases

* dbt incremental models
* CDC ingestion
* bronze → silver pipelines
* daily warehouse loads
* snapshot repair and reconciliation

Your roadmap’s dbt section will connect directly with this mindset, because dbt incremental models are really this exact strategy layer expressed declaratively. 

---

## Common interview questions

* full refresh vs incremental: when would you choose each?
* how do you handle late-arriving data?
* how do you make an incremental load idempotent?
* why is watermark-only loading risky?

---

## Strong mental model

When you design a recurring SQL pipeline, ask:

1. what identifies a row uniquely?
2. what marks new or changed data?
3. can data arrive late?
4. if the job reruns, will I duplicate data?
5. when should I rebuild fully instead?

That is data engineering thinking.

---

# 9) Partition Pruning, Clustering, and Warehouse-Aware SQL

## Intuition

In OLTP thinking, people often think:
“Use an index.”

In modern cloud warehouses, the thinking is more like:

* prune partitions / micro-partitions
* scan fewer files/blocks
* align filters with storage layout

Your optimization notes already introduce sargability and pruning. The missing part was the deeper interview framing around **partitioning and clustering strategy**.  

---

## Partition pruning

Partition pruning means the engine can skip scanning irrelevant chunks of data.

### Example

If a table is partitioned by `order_date`, this is good:

```sql
SELECT *
FROM sales
WHERE order_date >= DATE '2025-01-01'
  AND order_date < DATE '2025-02-01';
```

Why?
Because engine can read only January partitions.

---

## Bad version

```sql
SELECT *
FROM sales
WHERE YEAR(order_date) = 2025;
```

Now the engine may need to inspect far more data because you wrapped the column in a function.

This idea already exists in your optimization notes; what you needed was to link it explicitly to warehouse storage behavior and partition pruning. 

---

## Clustering / sort keys

Different warehouses use different terms:

* Snowflake: clustering keys
* Redshift: sort keys
* BigQuery: clustering + partitioning

The core idea is similar:

> organize physical storage so common filters and joins scan less data.

---

## Example thinking

If your largest table is frequently filtered by:

* `event_date`
* `customer_id`

Then those are strong candidates for partition/clustering design, depending on warehouse.

---

## Why this matters in interviews

If asked:
“How would you optimize a slow query on a huge fact table?”

A strong answer includes both:

* SQL rewrite
* storage/layout awareness

Example:

* filter early
* project only needed columns
* avoid functions on filter columns
* align filters with partition keys
* consider clustering/sort design for common predicates

---

## Common mistakes

### Mistake 1: thinking SQL text alone is enough

Sometimes performance depends on table design.

### Mistake 2: applying functions to partition key

Kills pruning.

### Mistake 3: selecting all columns

Columnar systems charge you for columns scanned too.

---

## Reusable mental model

Ask:

> “Can the engine skip most of the data, or am I forcing it to scan everything?”

That is the heart of warehouse optimization.

---

# 10) A Better “How to Think” Framework for Advanced SQL Interviews

This is not one syntax topic. It is the missing **meta-skill**.

Your notes already have good pattern thinking. What I want to add is the version that works especially well in interviews and production debugging.

---

## The 7-step mental checklist

When given any advanced SQL problem, think in this order:

### Step 1: Define the grain

What does one input row represent?

### Step 2: Define the output grain

What should one output row represent?

### Step 3: Identify the entity key

Per user? Per order? Per department?

### Step 4: Ask whether row order matters

Do I need ranking, prior row, running logic, session boundaries?

### Step 5: Estimate join cardinality

Will this join keep rows stable or multiply them?

### Step 6: Think about NULL behavior

Could NULL break join, filter, comparison, anti-join, or count?

### Step 7: Choose the right tool

* `GROUP BY`
* window
* anti-join
* set difference
* `MERGE`
* incremental filter
* pre-aggregation before join

This is how strong candidates stay calm.

---

# What to Memorize vs What to Understand

## Must memorize

These are worth being able to say/write from memory:

* window execution cannot be filtered in `WHERE`
* `QUALIFY` filters after window functions
* `COUNT(*)` vs `COUNT(col)`
* `NULL = NULL` is not true; use `IS NULL`
* `NOT EXISTS` is safer than `NOT IN` when NULLs may exist
* `ROWS` vs `RANGE`: `ROWS` is row-based, `RANGE` is value-based
* before joins, define grain and cardinality
* incremental pipelines need idempotency
* functions on partition/filter columns hurt pruning

---

## Must understand deeply

These should be concept-level, not rote syntax:

* why SQL execution order causes window filtering issues
* why `DISTINCT` is not a fix for a bad join
* how fan-out inflates metrics
* why NULL creates UNKNOWN, not FALSE
* why late-arriving data breaks naive watermark logic
* why warehouse performance depends on both query and storage layout
* why window functions are often better than correlated subqueries for analytics

---

# Interview-Quality Answer You Can Say Verbally

Here is a polished answer you can actually use:

> “For advanced SQL problems, I first define the grain of the input and the required grain of the output, because most mistakes come from grain mismatch or join fan-out. Then I check whether the problem is aggregation, row preservation with window functions, or a set/existence problem like an anti-join. I’m also careful about NULL semantics, because `NOT IN`, joins, and comparisons can silently behave differently when NULLs are present. For ranking and latest-row problems, I usually use `ROW_NUMBER`, then filter either with a CTE or `QUALIFY` in Snowflake. In production data engineering, I also think about incremental loading, idempotency, partition pruning, and whether my joins will multiply rows or scan too much data.”

That is a strong 15–18 LPA style answer.

---

# Practice Problems: Only on the Missing Areas

## 1. Easy

Given `employees(emp_id, dept, salary)`, return each employee row along with department average salary.

Use a window function, not `GROUP BY`.

---

## 2. Easy-Medium

Why does this query fail, and how do you fix it?

```sql
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
FROM orders
WHERE rn = 1;
```

Write both:

* portable fix
* Snowflake fix with `QUALIFY`

---

## 3. Medium

Given `customers(customer_id)` and `orders(order_id, customer_id)`, return customers with no orders using:

* `NOT EXISTS`
* `LEFT JOIN ... IS NULL`

Then explain which one you prefer and why.

---

## 4. Medium

Show with a small example why `NOT IN` can return wrong results when the subquery contains NULL.

Then rewrite it safely.

---

## 5. Medium-Hard

Given `sales(day, amount)` with duplicate `day` values, demonstrate the difference between:

```sql
SUM(amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

and

```sql
SUM(amount) OVER (ORDER BY day RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

Explain the tie behavior.

---

## 6. Hard

Given `orders(order_id, customer_id, order_amount)` and `order_items(order_id, item_id)`, show how a naive join inflates total order revenue. Then fix it.

---

## 7. Hard

Rewrite this correlated-subquery solution using a window function:

```sql
SELECT e.*
FROM employees e
WHERE salary > (
  SELECT AVG(salary)
  FROM employees e2
  WHERE e2.dept = e.dept
);
```

Explain why the window version is often better for analytics.

---

## 8. Hard

Design an incremental load for `source_orders(order_id, updated_at, amount)` into `target_orders`.

Explain:

* full refresh vs incremental
* watermark
* idempotency
* late-arriving data handling

---

## 9. Hard

A query on a huge `events` table filtered by `event_date` is slow. The current query is:

```sql
SELECT *
FROM events
WHERE YEAR(event_date) = 2025;
```

Explain why this hurts partition pruning and rewrite it properly.

---

# Final Mentor Note

The biggest gap was not syntax. It was **behavioral understanding**:

* when SQL stages happen
* when joins change grain
* when NULL breaks logic
* when warehouse design affects SQL performance
* when production pipelines need idempotent incremental thinking
