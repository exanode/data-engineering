# 📘 DE Mentor Session: Advanced SQL (Part 5)
**Topic:** Patterns (Gaps & Islands, SCD Type 2, MERGE)
**Goal:** Master the absolute highest-ROI interview patterns and real-world pipeline update mechanisms for Data Engineers (15-18 LPA).

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** Every DE needs to know two massive patterns:
    1.  **Gaps & Islands:** Identifying continuous sequences of data (Islands) and periods of missing data (Gaps) in time-series logs.
    2.  **Slowly Changing Dimensions (SCDs):** Keeping a historical track of updates. If an employee's salary changes, we don't `UPDATE` and overwrite the old salary. We insert a new row and expire the old one using a `MERGE` statement.
*   **Why it exists:** Modern businesses are obsessed with sequence analysis ("How many users went on a 7-day streak?"). Similarly, for compliance and accurate historical reporting, Data Warehouses *must* maintain a history of changes (SCD Type 2) rather than just the "current state."
*   **Where it's used:** Interview whiteboarding, building dimensional data models (Kimball methodology), and processing CDC (Change Data Capture) streams.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: The `MERGE` Statement**
The "Upsert." Instead of checking if a row exists, then branching to an `INSERT` or an `UPDATE`, `MERGE` handles both instantly.

**B. Intermediate: Implementing SCD Type 2**
Using `MERGE` combined with Window Functions (`LEAD()`) to close out an `effective_end_date` on an old record and insert a new `effective_start_date` record.

**C. Advanced: Gaps & Islands**
Using a math trick with `ROW_NUMBER()` to group consecutive dates together into named "Islands."

---

## 3. Concrete SQL Examples (Step-by-Step)

### Example A: The Gaps & Islands Math Trick
*Problem: Find users who logged in for at least 3 consecutive days.*
**Table:** `logins` (user_id, login_date)

**The Trick:** If you sequence rows 1, 2, 3... and subtract that sequence from a strictly increasing date... consecutive dates will yield the *exact same* baseline date!

```sql
WITH RankedLogins AS (
    SELECT 
        user_id,
        login_date,
        -- Generate the integer sequence
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY login_date) as rn,
        -- Subtract the sequence from the date. 
        -- If consecutive, the 'island_base_date' remains identical!
        DATEADD(day, -ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY login_date), login_date) as island_id
    FROM logins
)
SELECT 
    user_id, 
    MIN(login_date) as streak_start, 
    MAX(login_date) as streak_end,
    COUNT(*) as streak_length
FROM RankedLogins
GROUP BY user_id, island_id
HAVING COUNT(*) >= 3; -- Filter for 3-day streaks!
```

### Example B: The SCD Type 2 `MERGE`
We receive new employee data. If they exist, expire their old record.
```sql
MERGE INTO dim_employees target
USING incoming_hr_feed source
ON target.emp_id = source.emp_id AND target.is_current = TRUE
WHEN MATCHED AND target.salary != source.salary THEN
    -- In practice, you do a trick here to UPDATE the old row's end_date 
    -- and simultaneously INSERT the new row (handled beautifully by dbt snapshots).
    UPDATE SET is_current = FALSE, valid_to = CURRENT_DATE()
WHEN NOT MATCHED THEN
    INSERT (emp_id, salary, valid_from, is_current)
    VALUES (source.emp_id, source.salary, CURRENT_DATE(), TRUE);
```

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** Gaps & Islands is the most feared SQL interview question at FAANG. Memorize the `Date - ROW_NUMBER()` hack. 
*   **Real-world DE Use Case:** Every time a user updates their profile in the app, the CDC tool (Debezium) pushes the change. You use a `MERGE` statement in Snowflake to elegantly track the history of their profile changes without deleting anything.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Duplicate Rows in `MERGE`:** If your `USING` source data has duplicates on the join key, the `MERGE` will fail with an error because the target row doesn't know *which* source row to update it with. Always deduplicate your source data (using `ROW_NUMBER` rn=1) *before* the merge!
2.  **Missing Days in Islands:** The math trick only works if there is max ONE record per day. You must do a `SELECT DISTINCT date` aggregation step before attempting Gaps & Islands, otherwise `ROW_NUMBER` increments without the date incrementing, breaking the math.

---

## 6. Patterns & Mental Models
*   **The "Date-Minus-Rank" Hack:** Remember: A date stepping forward by 1 day MINUS an integer stepping forward by 1 equals a CONSTANT. That constant is your `GROUP BY` key for the island.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"State means current state. When an order status turns to Shipped, I run an `UPDATE` on the `status` column."*
*   **Data Engineer Mindset:** *"Data is immutable. Data scientists need to know exactly how long an order sat in 'Processing'. I never `UPDATE` inline; I use SCD Type 2 to append a new row and close the validity window of the old row."*

---

## 8. Practice Problems
1.  **Beginner:** Write a basic `MERGE` statement that inserts new users and updates the email address of existing users.
2.  **Advanced:** Create a dummy table with dates: Jan 1, Jan 2, Jan 3, Jan 6, Jan 7. Write the Gaps & Islands query to prove it creates two islands (Jan 1-3, Jan 6-7).

---

## 9. The Interview-Quality Verbal Answer
**Interviewer:** *"We need to track historical changes to our Salesforce Accounts over time. If an Account tier changes from Silver to Gold, how do we handle this in the data warehouse?"*
**You:** > "I would implement a Slowly Changing Dimension (SCD) Type 2. When the Gold update arrives in our staging area, I'd use a `MERGE` statement. For the matched Account ID where the tier has changed, I wouldn't overwrite the Silver record. Instead, I would update the Silver record's `effective_end_date` to today's date and set its `is_active` flag to false. Simultaneously, I would insert the new Gold record with an `effective_start_date` of today and a null end date. This preserves the perfect historical state for BI reporting."
