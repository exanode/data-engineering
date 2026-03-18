# 📘 DE Mentor Session: System Design (Part 1)
**Topic:** Architecture Patterns (Batch vs Streaming)
**Reference:** *Fundamentals of Data Engineering (Ch. 2-3)*
**Target:** 15-18 LPA Data Engineer

---

## 1. Intuitive Explanation: What & Why
*   **What it is:** System Design for Data Engineers is not about writing code; it's about drawing the blueprint. How does a single click on an iPhone eventually end up as a bar chart on the CEO's dashboard 10 seconds later? 
*   **Why it exists:** Typical Oracle APEX apps rely on a single massive relational database. This is a monolithic architecture. When Twitter receives 10,000 tweets per *second*, a single database physically melts. We have to architect explicitly for scale, deciding whether we process data constantly (Streaming) or in massive overnight chunks (Batch).
*   **Where it's used:** It is the core of Senior and Staff-level interviews. You will be given a whiteboard and told: *"Design the backend data system for Netflix."*

---

## 2. The Progression: Fundamentals → Intermediate → Advanced

**A. Fundamentals: Batch Processing**
The classic ELT pipeline. Data is collected, sits idle all day, and at 2:00 AM, an Airflow DAG triggers Spark/Snowflake to process 24 hours of data. (High Latency, High Throughput, Lower Cost).

**B. Intermediate: Stream Processing**
Processing data the exact millisecond it arrives. An event hits a Kafka Queue, a Spark Streaming job immediately processes it, and updates a Live Dashboard. (Low Latency, High Throughput, Higher Cost).

**C. Advanced: Lambda & Kappa Architectures**
*   **Lambda:** You build *both*. You have a Kafka streaming path for real-time approximations (e.g., Live View Count), and a heavy overnight Batch path that overwrites the streaming data with mathematically perfect numbers the next day.
*   **Kappa:** Batch is dead! You treat everything as a stream. If you need to recompute historical data, you just replay the Kafka stream from the very beginning.

---

## 3. Concrete Architectural Examples (Step-by-Step)

### Example: Architecting a Fraud Detection System
*Goal: If a user swipes a credit card in Mumbai and 2 seconds later in London, block the card immediately.*

**The Oracle/APEX approach (Fails at scale):**
```text
Mobile App -> Oracle DB (INSERT).
Every 60 seconds, an APEX Job runs: SELECT * FROM transactions WHERE ...
Result: The fraudster already bought the TV because 60 seconds is too late.
```

**The Kappa Streaming Architecture (Senior DE):**
```text
1. Mobile App -> AWS API Gateway
2. Gateway -> Apache Kafka (Publishes a 'TransactionEvent' message instantly).
3. Apache Flink / Spark Streaming (Subscribed to Kafka. Holds a rolling 5-minute memory window).
   - Instantly checks the physical distance between the last two swipes for this User_ID.
4. Flink -> Redis (Fast Key-Value Store). Flags user as "BLOCKED_FRAUD_TRUE".
5. Next swipe attempts to authenticate -> hits Redis -> instantly blocked in 10 milliseconds.
```

---

## 4. Connection to Interviews & Real World
*   **Interview Connection:** "We want to build a live leaderboard for our mobile game with 10 million players. Do you choose Batch or Streaming?" (Answer: Streaming. A leaderboard requires sub-second latency. I would propose an architecture using Kafka for event ingestion and a fast in-memory store like Redis to handle the sorted leaderboards).
*   **Real-world DE Use Case:** Uber's dynamic pricing (surge pricing). They cannot run a 1-hour batch job to decide if rides should cost more. They use a massive Kappa streaming architecture to constantly calculate the ratio of open apps to roaming drivers per geo-fence every 5 seconds.

---

## 5. Common Mistakes, Edge Cases, and Gotchas
1.  **"Let's Stream Everything":** The biggest junior mistake. Streaming architectures are fundamentally more complex, harder to debug, and much more expensive than Batch. If a dashboard only needs to be checked by Finance once a week, do not propose a complex Kafka streaming architecture! **Gotcha fix:** Only choose Streaming if the business SLA strictly requires sub-minute latency.
2.  **Out-of-Order Events:** In streaming, a user clicks "Buy" at 1:00pm, but their internet drops. Their phone reconnects and sends the event at 1:05pm. How does your backend handle data arriving late? (You must design "Watermarks" to handle late-arriving stream events).

---

## 6. Patterns & Mental Models
*   **The "Water Tap vs The Bucket" Model:** 
    *   **Streaming (Tap):** You leave the tap open and drink the water the exact second it falls out of the faucet.
    *   **Batch (Bucket):** You put a 5-gallon bucket under the tap. You wait all day for it to fill. Then at midnight, you dump the whole bucket into a filter at once.

---

## 7. How to Think About It (The APEX to DE Shift)
*   **Oracle Dev Mindset:** *"The database is the center of the universe. All applications write directly into the database tables."*
*   **Data Engineer Mindset:** *"The database is at the absolute end of the pipeline. Applications never write to the data warehouse directly. Applications write to a Message Queue (Kafka/Kinesis), which acts as a shock-absorber. We consume from that queue."*

---

## 8. Summary: Memorize vs. Understand
*   **Memorize:** Lambda Architecture = Batch Path + Speed Path. Kappa Architecture = Streaming Path Only.
*   **Understand:** Message Queues (Kafka) are the universal glue. They decouple the Producer (the Web App) from the Consumer (The Data Lake). If Snowflake goes offline for 3 hours, the Web App doesn't crash; the messages just wait safely inside Kafka until Snowflake returns.

---

## 9. Practice Problems
1.  **Trade-offs:** What are the trade-offs of the Lambda Architecture? Why did the industry create the Kappa architecture to replace it? *(Hint: Look up "managing two separate, identical codebases").*
2.  **System Design Prompt:** Architect a system for an E-commerce company. They need daily sales reports (for Finance) and instant "Out of Stock" alerts (for the Website backend). Which architecture pattern do you use?

---

## 10. The Interview-Quality Verbal Answer
**Interviewer:** *"If we are building a recommendation engine that needs to suggest products based on a user's clicks in the last 10 seconds, how do we architect the ingestion layer?"*
**You:** > "A traditional batch-oriented ETL pipeline processing data nightly into a data warehouse will not meet the sub-second latency SLA required for a live recommendation engine. We must adopt a Stream-processing architecture.
>
> I would decouple the frontend application from our databases by placing a distributed event streaming platform, like Apache Kafka or AWS Kinesis, as the central ingestion buffer. When a user clicks, the web app simply publishes an event to a Kafka topic. I would then deploy a stream processing engine like Apache Flink or Spark Structured Streaming to continually consume those events in real-time. Finally, the stream processor would evaluate the user's click history against our ML models and sink the resulting product recommendations into a low-latency NoSQL database like DynamoDB or Redis, which the frontend can query instantly."
