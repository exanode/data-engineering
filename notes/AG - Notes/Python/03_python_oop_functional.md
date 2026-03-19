# 📘 DE Mentor Session: Python for DE (Part 3)
**Topic:** Functions (`*args`, `**kwargs`, and Decorators)
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** In PL/SQL, when you declare a Procedure, you must strictly define every parameter it accepts (`p_name VARCHAR2`, `p_id NUMBER`). In Python, functions are highly dynamic. You can pass a variable number of arguments using `*args` and `**kwargs`. Furthermore, you can pass a function *into* another function (something impossible in legacy DB languages), leading to **Decorators**.
*   **Why it exists:** Data Engineers build re-usable tools. What if you want to write a custom logging function that can time *any* other Python function, regardless of how many parameters that function takes? You use `*args` to pass those wildcards, and `@decorators` to attach the logic.
*   **Where it's used:** Core to Airflow architecture. If you look at modern Airflow code (the TaskFlow API), every single pipeline is built using `@task` decorators!

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: Functions as First-Class Citizens**
In Python, a function is just an object. You can assign it to a variable, or pass it into a `map()` function. 
*Example:* `cleaned_data = map(upper_case_function, raw_list)`

**B. Intermediate: `*args` and `**kwargs`**
`*args` intercepts any number of positional variables and puts them in a Tuple.
`**kwargs` (Keyword Arguments) intercepts any number of explicitly named variables (`url="api.com", timeout=30`) and puts them in a Dictionary.

**C. Advanced: Decorators (`@`)**
A function that takes another function, modifies or wraps it with new instructions (like Adding Retries, or Logging), and returns it.

---

## 3. Concrete Python Examples (Step-by-Step)

### Example A: Building a generic API Request logger (args & kwargs)
```python
def make_api_call(url, **kwargs):
    print(f"Calling {url}...")
    
    # kwargs is just a dictionary! If the user passed 'timeout=50', it is here.
    if 'timeout' in kwargs:
         print(f"Warning: Custom timeout set to {kwargs['timeout']}")
    
    # We can pass these mystery kwargs straight into the Python requests library
    # response = requests.get(url, **kwargs)

# Calling it cleanly
make_api_call("https://snowflake.com", timeout=120, verify_ssl=False)
```

### Example B: The Retry Decorator (Airflow style!)
If an API fails, DE pipelines shouldn't just crash. They should retry. We can build an `@retry` wrapper!

```python
import time

def retry_3_times(func):
    def wrapper(*args, **kwargs): 
        for attempt in range(3):
            try:
                return func(*args, **kwargs) # Try to execute the wrapped function
            except Exception as e:
                print(f"Failed attempt {attempt+1}. Retrying...")
                time.sleep(2)
        raise Exception("Failed after 3 retries!")
    return wrapper

# Using the decorator:
@retry_3_times
def unstable_api_call():
    # randomly fail logic here...
    return "Success!"
```

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "Write a decorator that times how long a function takes to execute." (A classic Python screening test).
*   **Real-world DE Use Case:** In Apache Airflow (the industry standard DE orchestrator), tasks are defined like this:
```python
from airflow.decorators import task

@task(retries=3)
def extract_salesforce_data():
    pass 
``` 

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **Parameter Ordering:** Python strictly enforces rule ordering. Positional arguments must come first, followed by `*args`, then explicit keywords, then `**kwargs`. (e.g., `def func(a, b, *args, **kwargs)`). If you mix them up, it fails with a `SyntaxError`.
2.  **Mutable Default Arguments (The biggest Python Gotcha!):**
    ```python
    # DO NOT DO THIS!
    def add_item(item, basket=[]):
        basket.append(item)
    ```
    If you use an empty list `[]` as a default parameter, Python instantiates that list *once at compile time*. Successive calls will all share the exact same list, leading to horrific data contamination! **Gotcha fix:** Use `basket=None`, and inside the function write `if basket is None: basket = []`.

---

## 6. Patterns & Mental Models
*   **The "Wrapper" Mental Model (Decorators):** Think of a Decorator like a gift-wrapping service. You provide the box (the core function). The decorator wraps it in shiny paper (logging, retries, authentication checks), and hands the wrapped box back identically functional, but equipped with new features.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"To add auditing to a procedure, I write `INSERT INTO audit_log` at the very start of the procedure body, and `COMMIT` at the end."*
*   **Data Engineer Mindset:** *"Writing boilerplate audit code inside 50 different functions violates the DRY (Don't Repeat Yourself) principle. I will write an `@audit_log` Python decorator exactly once, and simply slap the `@` symbol above any function I want automatically audited."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** Never use `[]` or `{}` as default arguments in Python functions.
*   **Understand:** `*args` represents a Tuple of positional items. `**kwargs` represents a Dict of named items.

---

## 9. Practice Problems
1.  **Decoding:** What does this output? 
    ```python
    def my_func(*args): print(args[1])
    my_func('A', 'B', 'C')
    ```
2.  **Advanced Design:** Write a basic decorator `@time_it` that imports the `time` module, records the `start_time`, runs the `func(*args, **kwargs)`, records the `end_time`, prints the difference, and returns the result.

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"What are `*args` and `**kwargs` in Python, and why do we use them extensively in Airflow and pipeline architectures?"*
**You:** > "`*args` and `**kwargs` allow us to pass a dynamic, variable number of arguments into a function. `*args` captures un-named positional arguments into a Tuple, while `**kwargs` captures named keyword arguments into a Dictionary. 
>
> We use them extensively in Data Engineering—especially when building framework operators or Decorators—because they allow us to write highly generic, reusable 'wrapper' code. For example, if I build a custom `SalesforceToS3` ingestion class, I can accept `**kwargs` and pass those mystery arguments directly down to the underlying `requests.get()` module without having to hardcode every possible network parameter like timeouts, headers, or proxies into my top-level class definition."
