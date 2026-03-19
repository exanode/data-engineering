# 📘 DE Mentor Session: Data Modeling (Part 2)
**Topic:** Facts & Dimensions (The Building Blocks)
**Reference:** *The Data Warehouse Toolkit (Chapter 1 & 2)*
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** A Star Schema has two types of tables. **Fact Tables** collect the numerical measurements of a business event (the verbs). **Dimension Tables** provide the context (the nouns: who, what, where, when). 
*   **Why it exists:** By separating the numbers (Facts) from the text strings (Dimensions), the database engine can sum up billions of tiny numerical integers instantly, only fetching the heavy text strings at the very end of the query from the Dimension table.
*   **Where it's used:** Transforming raw API JSON and relational tables into a schema ready for PowerBI or Tableau reporting.

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: The Dimension Table**
Wide, flat tables packed with text attributes. They are heavily indexed. They are the "Filter" and "Group By" targets. 
*Example:* `dim_customer` (Customer_SK, Name, Email, Region, Acquisition Channel).

**B. Intermediate: The Fact Table**
Deep, narrow tables. They contain ONLY three things: Surrogate Keys (mapping to dimensions), Degenerate Dimensions (like a raw Order Invoice Number), and Numerical Measures (Amount, Quantity).
*Example:* `fact_sales` (Date_SK, Customer_SK, Product_SK, Order_ID, Total_Amount, Qty, Tax).

**C. Advanced: Conformed Dimensions**
Using the *exact same* Dimension table across multiple different Fact tables.
*Example:* The same `dim_date` is used by `fact_sales` and `fact_hr_hires`. This allows the CEO to drill across both departments in one single report!

---

## 3. Concrete SQL Examples (Step-by-Step)

### Creating the Dimension Table
Notice how we flatten the `City` and `State` into the exact same table. We don't care about redundancy.
```sql
CREATE TABLE dim_store (
    store_sk INT PRIMARY KEY,         -- Surrogate Key (Meaningless integer)
    store_id VARCHAR(50),             -- Natural Key (From the Operations App)
    store_name VARCHAR(100),          -- Text Attribute
    city VARCHAR(50),                 -- Flattened geographic data!
    state VARCHAR(50),
    manager_name VARCHAR(100)
);
```

### Creating the Fact Table
Notice there are no text strings here (except maybe the order ID). Just keys and numbers.
```sql
CREATE TABLE fact_store_sales (
    sales_sk INT PRIMARY KEY,
    date_sk INT,                      -- FK to dim_date
    store_sk INT,                     -- FK to dim_store
    product_sk INT,                   -- FK to dim_product
    order_number VARCHAR(50),         -- Degenerate Dimension
    sales_amount DECIMAL(10, 2),      -- Additive Measure
    quantity INT                      -- Additive Measure
);
```

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** Interviewers love to give you a scenario ("We are building an Uber clone") and ask you to whiteboard the Fact and Dimension tables. **Golden Rule:** Identify the *grain* of the fact table first! (e.g., "1 row equals 1 completed ride").
*   **Real-world DE Use Case:** In dbt (Data Build Tool), you write SQL logic to populate these dimensions nightly. For instance, creating `dim_user` by `LEFT JOIN`ing the raw `users` table with the raw `stripe_customers` table.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Putting Text inside a Fact Table:** If you put the string "Laptop" into a 10-billion-row Fact table, you waste massive storage, kill cache performance, and defeat the purpose of the Star Schema. Replace "Laptop" with the integer `product_sk`, and put "Laptop" in `dim_product`.
2.  **Mismatched Grain:** The most common project failure. A Fact table grain must be uniform. Do not mix "Daily Subtotal" rows and "Individual Line Item" rows in the same fact table. If you sum it, you'll double-count the revenue!

---

## 6. Patterns & Mental Models
*   **The "Measure" Test:** If an attribute makes sense to put inside a `SUM()`, `AVG()`, or `MAX()` function, it usually belongs in the Fact table. If it makes sense to put inside a `WHERE` or `GROUP BY` clause, it goes in the Dimension.
*   **Degenerate Dimensions:** An ID (like `Invoice_Number`) that doesn't have a whole dimension table dedicated to it, but also isn't a measurable number. We leave it right inside the Fact table.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"I view an Order as an Object. It has a header table, a items table, a shipping address table."*
*   **Data Engineer Mindset:** *"I view an Order as a measurable 'Event'. The Fact table is logging the event itself. Every event is surrounded by the context (Dimensions) at the exact moment the event occurred."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** Facts = Numbers/Measures. Dimensions = Text/Context.
*   **Understand:** Conformed Dimensions. By sharing `dim_date` and `dim_user` globally across the company, marketing and finance can finally agree on the same numbers.

---

## 9. Practice Problems
1.  **Mental Exercise:** For Netflix, design the `fact_movie_stream` table. What is the grain? What are the foreign keys? What are the measures?
2.  **Design Exercise:** You have an App logging temperature sensors every minute. Is `Temperature_Celsius` a Fact measure or a Dimension attribute? (Hint: Does it make sense to AVG it?)

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"Can you explain the concept of Grain in Data Modeling, and why it is so critical?"*
**You:** > "The grain of a fact table is the exact business definition of what a single row represents. For example, in an e-commerce model, is the grain 'one row per order', or 'one row per line-item inside an order'? Defining the grain is the mandatory first step in Kimball dimensional modeling. If you design a fact table with a mixed or ambiguous grain, analysts will inevitably run aggregations that double-count revenue or mismatch foreign keys, completely breaking trust in the data warehouse."
