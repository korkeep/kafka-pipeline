### KAFKA-PIPELINE

Building a pipeline to effectively process bigdata using HDFS and Kafka 🐘

##### Hadoop Ecosystem

| Function                       | Project Name  | Description                                                                                           |
| ------------------------------ | ------------- | ----------------------------------------------------------------------------------------------------- |
| Distributed Coordinator        | **ZooKeeper** | Provides coordination services such as configuration management, synchronization, and leader election |
| Data Ingestion                 | **Kafka**     | A high-throughput, fault-tolerant platform for real-time data streaming                               |
| Distributed Data Storage       | **HDFS**      | A fault-tolerant distributed file system for large-scale data storage                                 |
| Distributed Cluster Management | **YARN**      | Manages and schedules cluster resources for distributed applications                                  |
| Distributed Batch Processing   | **MapReduce** | Processes large datasets in parallel using a distributed batch model                                  |
| In-memory Data Processing      | **Spark**     | An in-memory engine for fast distributed data processing and analytics                                |
| Data Warehouse Integration     | **Hive**      | Enables SQL-based analytics on data stored in HDFS                                                    |
| Data Warehouse Integration     | **Impala**    | An MPP-based SQL engine for low-latency interactive analytics                                         |
| Data Storage                   | **HBase**     | A distributed NoSQL database for real-time read and write operations                                  |
| Data Storage                   | **Kudu**      | A columnar storage system supporting fast updates and analytical queries                              |

##### HDFS vs HBase vs Kudu: Distributed Data Storage

| Category             | HDFS                      | HBase                               | Kudu                      |
| -------------------- | ------------------------- | ----------------------------------- | ------------------------- |
| Storage Type         | File-based                | Column-family-based                 | Columnar storage          |
| Data Model           | Files (block-based)       | Key-Value (RowKey)                  | Tables (rows and columns) |
| Read/Write Pattern   | Write once, read many     | Random read/write                   | Insert, update, delete    |
| Real-time Processing | ❌ Not supported          | ✅ Supported                        | ✅ Supported              |
| Batch Analytics      | ✅ Optimized              | ⚠️ Inefficient                      | ✅ Supported              |
| SQL Support          | ❌ Not supported directly | ❌ Limited                          | ✅ Supported (via Impala) |
| Latency              | High                      | Low                                 | Low                       |
| Updates              | ❌ Not supported          | ✅ Supported                        | ✅ Supported              |
| Typical Use Cases    | Logs, images, large files | Real-time lookups, time-series data | Real-time analytics, OLAP |
| Common Integrations  | Hive, Spark               | HBase API                           | Impala, Spark             |

##### Data Warehouse vs Data Lake

| Category    | Data Warehouse                                  | Data Lake                                          |
| ----------- | ----------------------------------------------- | -------------------------------------------------- |
| Data Type   | Primarily structured data                       | Structured, semi-structured, and unstructured data |
| Schema      | Schema-on-Write (schema defined at ingestion)   | Schema-on-Read (schema applied at query time)      |
| Hardware    | High-performance servers (traditional scale-up) | Commodity hardware (scale-out)                     |
| Scalability | Well-suited for vertical scaling (scale-up)     | Well-suited for horizontal scaling (scale-out)     |
| Software    | RDBMS, MPP databases (Oracle, Teradata, etc.)   | Hadoop, Spark, HDFS, Object Storage                |

##### YARN: Distributed Data Processing

| Component           | Role                    | Description                                                      |
| ------------------- | ----------------------- | ---------------------------------------------------------------- |
| Resource Manager    | Cluster manager         | Manages overall cluster resources and application lifecycle      |
| Scheduler           | Resource allocator      | Allocates resources to applications based on scheduling policies |
| Application Manager | Application controller  | Handles application submission and starts Application Masters    |
| Node Manager        | Node agent              | Manages node-level resources and runs containers                 |
| Application Master  | Application coordinator | Requests resources and manages application execution             |
| Container           | Execution unit          | A resource-isolated environment where tasks run                  |
