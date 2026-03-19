# 📘 DE Mentor Session: Advanced SQL (Part 3)
**Topic:** OLAP Aggregations (`GROUPING SETS`, `ROLLUP`, `CUBE`)
**Goal:** Learn how to aggregate data at multiple hierarchical levels in a single scan for Data Warehouse reporting (15-18 LPA).

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** A standard `GROUP BY` returns one level of aggregation (e.g., total sales per City). If you also need total sales per State, and Grand Total sales, you'd traditionally write three separate queries and `UNION ALL` them. OLAP functions like `ROLLUP` and `CUBE` are syntactic sugar that do this all in one pass.
*   **Why it exists:** BI tools (Tableau, PowerBI) need "One Big Table" (OBT) with pre-aggregated subtotals to load dashboards quickly. If your DE pipeline hits the 10-terabyte fact table three separate times for three `UNION`s, performance collapses.
*   **Where it's used:** Building Materialized Views, Aggregation Tables (e.g., `daily_sales_summary`), and serving data directly to BI tools.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: `GROUPING SETS`**
Explicitly define exactly which groupings you want.
*Example:* `GROUP BY GROUPING SETS ((City), (State), ())` -> Gives City totals, State totals, and the Grand Total `()`.

**B. Intermediate: `ROLLUP` (Hierarchical)**
Automatically calculates subtotals moving from right to left, plus a grand total. Best for hierarchical data.
*Example:* `ROLLUP (Year, Month, Day)` -> Yields (Year, Month, Day), (Year, Month), (Year), and Grand Total.

**C. Advanced: `CUBE` (Cross-Tabular)**
Generates every mathematical combination of the columns. 
*Example:* `CUBE (Store, Product)` -> Yields (Store, Product), (Store), (Product), and Grand Total. 

---

## 3. Concrete SQL Examples (Step-by-Step)

### Example A: The `ROLLUP` Hierarchy
Imagine a sales table (`order_date`, `region`, `amount`).
```sql
SELECT 
    EXTRACT(YEAR FROM order_date) as yr,
    region,
    SUM(amount) as total_sales
FROM sales
GROUP BY ROLLUP (EXTRACT(YEAR FROM order_date), region);
/* 
Output includes:
- 2023, North: $500 (Base level)
- 2023, NULL:  $500 (Subtotal for Year 2023)
- NULL, NULL:  $500 (Grand Total of everything)
*/
```

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "We need a report showing sales by Product Category, by Store, and the overall total. How do you do this without `UNION`?" (Answer: `CUBE(Category, Store)`).
*   **Real-world DE Use Case:** Creating "Summary Tables". Instead of letting Tableau query millions of rows, the DE runs a daily Airflow job using `ROLLUP` to materialize a tiny aggregate table.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **The `CUBE` Explosion:** If you `CUBE(A, B, C, D, E)`, it generates 2^5 = 32 different grouping sets. If those columns have high cardinality (many distinct values), your cluster will run out of memory (OOM) and crash. Only `CUBE` low-cardinality dimensions.
2.  **Confusing NULLs:** `ROLLUP` outputs `NULL` to represent "All". If your underlying data actually has `NULL` regions, you won't know if the `NULL` means "Grand Total" or "Missing Region". Use the `GROUPING(column_name)` function to differentiate them (returns 1 for generated subtotals, 0 for actual data).

---

## 6. Patterns & Mental Models
*   **The "Hierarchy Filter" Pattern:** Use `ROLLUP` for Time (Year -> Month -> Day) or Geography (Country -> State -> City). Use `CUBE` for independent dimensions (Device Type x Traffic Source).

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"I've built an APEX interactive grid that calculates subtotals on the frontend using JavaScript or native APEX features."*
*   **Data Engineer Mindset:** *"The BI tool is struggling to compute subtotals on 5 Billion rows. I will pre-compute all required dimensional slices natively in the Data Warehouse using `CUBE` inside my dbt model."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** `ROLLUP` is directional (A, B, C -> A, B -> A -> Total). `CUBE` is total permutation (A, B -> A -> B -> Total).
*   **Understand:** These are just engine-optimized shortcuts for massive `UNION ALL` statements.

---

## 9. Practice Problems
1.  Write a query using `GROUPING SETS` to find total employees by Department *and* total employees by Job Title, but NOT the combination of both.
2.  Use `ROLLUP` to find total regional sales by Year and Quarter.
3.  Write a query highlighting the difference in output size between `CUBE(A, B)` and `ROLLUP(A, B)`.

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"What is the difference between `CUBE` and `ROLLUP`, and when would you avoid `CUBE`?"*
**You:** > "`ROLLUP` generates hierarchical subtotals—it drops columns from right to left. It’s perfect for time or geographic hierarchies. `CUBE` generates every possible combination of subtotals across all specified columns. I would actively avoid `CUBE` if the columns have high cardinality, because generating 2^N combinations across millions of distinct values causes massive data explosions, leading to out-of-memory errors on the cluster."
