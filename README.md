# PYSPARK

# 1. Introduction to PySpark
# 1.1 PySpark Overview

* PySpark is the Python API for Apache Spark.

* It allows you to use Python to write Spark jobs.

 Spark is a big data processing engine used for:

* Handling large datasets

* Distributed processing (parallel processing)

* Faster performance compared to Hadoop MapReduce

 * PySpark = Spark + Python


# 1.2 Role of PySpark in Big Data Processing

* PySpark helps in:Processing huge data (GBs, TBs and even PBs)

* Distributed computing (split data into partitions)

# Doing operations like:

*Filtering

* Joins

* Aggregations

* Machine learning (MLlib)

* SQL queries (Spark SQL)

* Example:
If you have a 1 TB file, Python can't load it.
PySpark splits it into smaller parts and processes them on multiple machines.

# 1.3 Python API for Apache Spark

PySpark gives Python users access to:

* Spark Core API (RDDs)

* Spark SQL API (DataFrame, SQL)

* Spark Streaming

* Spark MLlib

* GraphX-like API using GraphFrames


# Spark Architecture (Revision)
# 2.1 Spark Architecture Overview

| Component           | Role                                                      |
| ------------------- | --------------------------------------------------------- |
| **Driver Program**  | Controls the job, creates RDD/DataFrame, schedules tasks  |
| **Cluster Manager** | Allocates resources (YARN, Mesos, Kubernetes, Standalone) |
| **Executors**       | Workers that actually run the tasks                       |
| **Tasks**           | Small units of work executed on executors                 |



* Driver converts code into tasks(It creates tasks and sends them for execution.”

* The Cluster Manager allocates resources and assigns executors.

* The Executors run tasks on worker nodes and return results.”

# 3. Spark Components
# * spark core
base engine of apache spark , every other spark (sql, ste....) is top of spark core
it handle:
* Memory management
  
* Task scheduling

* Fault tolerance

# * Spark SQL
Spark SQL is a module used for structured data processing using:

* SQL queries

* DataFrames

# * spark Streaming 
processes real-time data in small batches (micro-batching).
kafka (distributed streaming platform used for handling real-time data.)

# * MLlib 
Machine library is Spark’s built-in machine learning library.

it supports: classification , cluster

#  *	graphx 

 * it graph processing libary 
 * it allow you to work in graph(nodes, edge)
 
# Example

*find shortest path , analysis networkkk




