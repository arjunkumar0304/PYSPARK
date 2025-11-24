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

# 4.sparksession:

* SparkSession is the unified entry point for working with PySpark.
* It allows us to create DataFrames, execute SQL queries, read files, and connect with the Spark cluster. Internally, SparkSession contains SparkContext.


Configuring and Creating a SparkSession:

<img width="812" height="207" alt="image" src="https://github.com/user-attachments/assets/6d3889bd-e21d-470c-8652-9f882336de79" />


# 5.DataFrame API
Overview of the DataFrame API in PySpark

A DataFrame in PySpark is:

✔ A distributed collection of data
✔ Organized into rows and columns
✔ Similar to a SQL table

#  Comparison with Pandas DataFrames


| Feature            | Pandas DataFrame             | PySpark DataFrame                                                  |
| ------------------ | ---------------------------- | ------------------------------------------------------------------ |
| **Scale**          | Works on single machine      | Distributed across cluster                                         |
| **Data Size**      | Small/medium data            | Big data (GB–TB–PB)                                                |
| **Speed**          | Slower for large data        | Faster due to parallel processing                                  |
| **Lazy Execution** | No                           | Yes (runs only when action like `.show()` or `.count()` is called) |
| **Optimization**   | No optimizer                 | Catalyst optimizer      |
| **Syntax**         | Pythonic operations          | SQL-like + Pythonic   


# 6. Transformations and Actions in PySpark


* Transformations are operations that create a new RDD/DataFrame from an existing one.
* They are lazy, meaning Spark does not execute them immediately.
* Execution happens only when an action is called.


# action:
action is a operation trigger execution in spark

| Action                   | Purpose                                   |
| ------------------------ | ----------------------------------------- |
| `collect()`              | Return all data to driver (use carefully) |
| `count()`                | Count number of records                   |
| `take(n)`                | Take first n records                      |
| `reduce()`               | Reduce values to a single output          |
| `show()` (for DataFrame) | Display data                              |
| `write`                  | Save to file/table                        |



# 7. spark RDDs (Resilient Distributed Datasets)

7.1 Overview of RDDs in PySpark

* RDD (Resilient Distributed Dataset)(map ,filter, reduce)

* RDD is the lowest-level Spark abstraction.

* It stores unstructured data as Java/Python objects.

* It gives full control over data but has no optimization.

* No schema, no optimization by Catalyst.

Overview of RDDs in PySpark

Differences between RDDs and DataFrames.
 

| Feature      | RDD                       | DataFrame                   |
| ------------ | ------------------------- | --------------------------- |
| Structure    | Unstructured              | Structured (rows & columns) |
| Speed        | Slower                    | Faster (Catalyst Optimizer) |
| Syntax       | Python functions          | SQL-like operations         |
| Use Case     | Low-level transformations | Big data analytics          |
| Optimization | No                        | Yes                         |


