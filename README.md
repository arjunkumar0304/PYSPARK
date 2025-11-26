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


# 8.PySpark Data Structures

* RDD (Resilient Distributed Dataset)

* DataFrame

* Dataset


# RDD (Resilient Distributed Dataset)
* RDD is the fundamental data structure in Spark.
* It represents a distributed collection of objects divided across multiple machines.

# Characteristics

*Low-level API

* No predefined schema

* Immutable (cannot be changed once created)

* Provides transformations and actions

* Suitable for unstructured data

       rdd = spark.sparkContext.parallelize([1, 2, 3, 4])


# Dataframe

A DataFrame is a distributed table-like structure with rows and columns, similar to a SQL table or a Pandas DataFrame.

# Characteristics

High-level API

Has schema (column names + data types)

Uses Catalyst Optimizer (high performance)

Supports SQL operations

Recommended for most PySpark workloads

```
  data = [(1, "Arjun"), (2, "Kumar")]
df = spark.createDataFrame(data, ["id", "name"])
df.show()
```


# Dataset

A Dataset is a strongly-typed structured collection of data.

# Key Points

Provides compile-time type safety

Combines benefits of RDD and DataFrame

Fully supported only in Scala/Java

In PySpark, DataFrame = Dataset<Row>

* PySpark Note : In PySpark, Datasets are NOT used because Python does not support compile-time types.

# Comparison Between RDD, DataFrame, Dataset


| Feature        | RDD          | DataFrame            | Dataset     |
| -------------- | ------------ | -------------------- | ----------- |
| Data Structure | Objects      | Table (rows/columns) | Typed table |
| Ease of Use    | Hard         | Easy                 | Medium      |
| Schema         | No           | Yes                  | Yes         |
| Performance    | Slow         | Fast                 | Fast        |
| Optimization   | No           | Yes (Catalyst)       | Yes         |
| API Support    | Python/Scala | Python/Scala         | Scala/Java  |
| Type Safety    | No           | No                   | Yes         |


# 9. SparkContext

*  The Role of SparkContext in PySpark

SparkContext = the connection between your PySpark program and the Spark Cluster.

Think of it like:

SparkSession → High-level API for SQL, DataFrames

SparkContext → Low-level API for RDD operations


```
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("MyApp")     .master("local[*]")     .getOrCreate()

sc = spark.sparkContext

```

Configuration Options:

.appName("MyApp") → Identifies your job.
.master("local[*]") → Runs locally using all CPU cores.
Additional configs like memory, executor cores, etc.


# 10.PySpark DataFrames

# 10.1 Introduction to PySpark DataFrames

A DataFrame in PySpark is:

✔ A table-like structure (rows & columns)
✔ Similar to Pandas DataFrame, but works on big data
✔ Distributed across multiple machines in a cluster
✔ Highly optimized using Catalyst Optimizer


<img width="1202" height="242" alt="image" src="https://github.com/user-attachments/assets/8c556c3b-5d68-4475-92bf-4ade05be9e9e" />


# 10.2 Operations on DataFrames


<img width="1178" height="985" alt="image" src="https://github.com/user-attachments/assets/214683dd-7a58-4275-8cd7-d4bb208af9f5" />


| Operation      | Method                      |
| -------------- | --------------------------- |
| Select columns | `select()`                  |
| Filter rows    | `filter()`, `where()`       |
| Sort rows      | `orderBy()`                 |
| Group data     | `groupBy()`                 |
| Aggregate      | `count, sum, avg, max, min` |
| Add new column | `withColumn()`              |


# 11.PySpark SQL

PySpark allows you to run SQL queries on DataFrames just like you do in a database

Run SQL queries

Create temporary views

Join DataFrames using SQL

Use functions (SUM, AVG, COUNT, etc.)



<img width="862" height="493" alt="image" src="https://github.com/user-attachments/assets/5994526e-fd1c-43b3-84c6-e5719db92b86" />


# Caching & Persisting

Spark recomputes transformations every time an action (like count() or show()) is triggered.

To speed up repeated operations:

Use cache() to store results in memory.
Use persist() for more storage control (memory, disk, etc.).


```
df.cache()
```

Without cache → Spark recomputes every time.
With cache → Spark stores results in memory → no recomputation.

# persist|

persist() for more storage control (memory, disk)
persist() allows you to specify the storage level (memory, disk, serialization).


| Feature       | cache()     | persist()                         |
| ------------- | ----------- | --------------------------------- |
| Storage Level | Only MEMORY | You can choose MEMORY, DISK, both |
| Usage         | Simple      | Advanced control                  |



# pyspark dataframe function



<img width="951" height="1070" alt="Screenshot 2025-11-26 172347" src="https://github.com/user-attachments/assets/6861dc8c-fbcc-42d7-8aee-1b6d60c77eaf" />



| **Function**           | **Purpose / What It Does**                        |
| ---------------------- | ------------------------------------------------- |
| **show()**             | Displays DataFrame rows in table format           |
| **collect()**          | Returns *all* rows to the driver as a Python list |
| **take(n)**            | Returns *first n rows* (safer than collect)       |
| **printSchema()**      | Prints schema: column names + data types          |
| **count()**            | Returns number of rows                            |
| **select()**           | Selects specific columns                          |
| **filter() / where()** | Filters rows based on a condition                 |
| **like()**             | Pattern matching (like SQL LIKE)                  |
| **sort() / orderBy()** | Sorts DataFrame rows                              |
| **describe()**         | Shows summary statistics (count, mean, min, max)  |
| **columns**            | Returns list of column names                      |




# pyspark string function

<img width="915" height="919" alt="Screenshot 2025-11-26 182849" src="https://github.com/user-attachments/assets/40004765-10e0-4765-b168-32e876a813d7" />



| **Function**          | **Purpose**                        | **Example Code**                         | **Output Explanation**                    |
| --------------------- | ---------------------------------- | ---------------------------------------- | ----------------------------------------- |
| **upper()**           | Convert to UPPERCASE               | `upper(col("name"))`                     | arjun → ARJUN                             |
| **lower()**           | Convert to lowercase               | `lower(col("name"))`                     | ARJUN → arjun                             |
| **trim()**            | Remove left + right spaces         | `trim(col("name"))`                      | `"  arjun  "` → `"arjun"`                 |
| **ltrim()**           | Remove LEFT spaces                 | `ltrim(col("name"))`                     | `"  arjun"` → `"arjun"`                   |
| **rtrim()**           | Remove RIGHT spaces                | `rtrim(col("name"))`                     | `"arjun  "` → `"arjun"`                   |
| **length()**          | Count characters                   | `length(col("name"))`                    | "arjun" → 5                               |
| **substring()**       | Extract part of string             | `substring(col("name"),1,3)`             | "arjun" → "arj"                           |
| **substring_index()** | Extract before/after delimiter     | `substring_index(col("email"),"@",1)`    | "[a@gmail.com](mailto:a@gmail.com)" → "a" |
| **split()**           | Split into array                   | `split(col("email"),"@")`                | `["a","gmail.com"]`                       |
| **repeat()**          | Repeat string                      | `repeat(col("name"),2)`                  | "arjun" → "arjunarjun"                    |
| **lpad()**            | Pad LEFT side                      | `lpad(col("name"),10,"*")`               | "*****arjun"                              |
| **rpad()**            | Pad RIGHT side                     | `rpad(col("name"),10,"*")`               | "arjun*****"                              |
| **regex_replace()**   | Replace using regex                | `regex_replace(col("name"),"a","A")`     | "arjun" → "Arjun"                                                   |
| **instr()**           | Find index of substring            | `instr(col("name"),"a")`                 | "arjun" → 1                               |
| **initcap()**         | Capitalize first char of each word | `initcap(col("name"))`                   | "mahendra" → "Mahendra"                   |

