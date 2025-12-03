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


# 12.Caching & Persisting

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



# 13. pyspark dataframe function



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




# 14.pyspark string function

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

# 15. Numeric Function



<img width="620" height="647" alt="Screenshot 2025-11-27 182008" src="https://github.com/user-attachments/assets/1304b904-e8db-49fe-9a01-1f74851a480d" />



| Function    | Purpose         | PySpark Example          |
| ----------- | --------------- | ------------------------ |
| **SUM()**   | Total of values | `sum("salary")`          |
| **AVG()**   | Average value   | `avg("salary")`          |
| **MIN()**   | Smallest value  | `min("salary")`          |
| **MAX()**   | Largest value   | `max("salary")`          |
| **ROUND()** | Round decimals  | `round(col("value"), 2)` |
| **ABS()**   | Absolute value  | `abs("value")`           |


# 16. Date and time function





<img width="1258" height="895" alt="Screenshot 2025-11-27 182131" src="https://github.com/user-attachments/assets/ea4d4ed6-cc9f-42fa-a359-4222e96d7c64" />



| **Function**                       | **Meaning / Purpose**                | **Example Code**                         | **Output Example**    |
| ---------------------------------- | ------------------------------------ | ---------------------------------------- | --------------------- |
| **CURRENT_DATE()**                 | Returns today’s system date          | `current_date()`                         | `2025-11-27`          |
| **CURRENT_TIMESTAMP()**            | Returns current date with time       | `current_timestamp()`                    | `2025-11-27 16:55:10` |
| **DATE_ADD(col, days)**            | Adds N days to a date                | `date_add(col("date"), 10)`              | `2024-11-30`          |
| **DATEDIFF(date1, date2)**         | Difference in days between two dates | `datediff(current_date(), col("date"))`  | `372`                 |
| **YEAR(col)**                      | Extracts year from date              | `year(col("date"))`                      | `2024`                |
| **MONTH(col)**                     | Extracts month number (1–12)         | `month(col("date"))`                     | `11`                  |
| **DAY(col)** / **DAYOFMONTH(col)** | Extracts day of month                | `dayofmonth(col("date"))`                | `20`                  |
| **TO_DATE(col)**                   | Converts string → date               | `to_date(col("date"))`                   | `2024-11-20`          |
| **DATE_FORMAT(col, format)**       | Converts date into custom format     | `date_format(col("date"), "dd/MM/yyyy")` | `20/11/2024`          |


# 17. Aggregate function

<img width="594" height="934" alt="Screenshot 2025-11-27 182331" src="https://github.com/user-attachments/assets/d7f57009-8de9-416b-b989-7f2dc88e3ff8" />




| Function            | Description                                    | Example                 | Output Meaning            |
| ------------------- | ---------------------------------------------- | ----------------------- | ------------------------- |
| **mean()**          | Calculates average value                       | `mean("salary")`        | Average salary            |
| **avg()**           | Same as mean()                                 | `avg("salary")`         | Average salary            |
| **sum()**           | Total of all values                            | `sum("salary")`         | Total salary amount       |
| **min()**           | Minimum value                                  | `min("salary")`         | Lowest salary             |
| **max()**           | Maximum value                                  | `max("salary")`         | Highest salary            |
| **count()**         | Counts number of rows                          | `count("id")`           | No. of employees          |
| **countDistinct()** | Counts unique values                           | `countDistinct("dept")` | No. of unique depts       |
| **first()**         | First value in column                          | `first("salary")`       | First salary in DataFrame |
| **last()**          | Last value in column                           | `last("salary")`        | Last salary in DataFrame  |
| **collect_list()**  | Collects values into list (duplicates allowed) | `collect_list("name")`  | All names in list         |
| **collect_set()**   | Collects unique values (removes duplicates)    | `collect_set("name")`   | Unique names only         |



# 18. Joins


<img width="982" height="811" alt="Screenshot 2025-11-28 142836" src="https://github.com/user-attachments/assets/f772fcbd-7ef8-461a-b8ea-1faccb3abb91" />


| Join Type           | Returns                                           | Example Use                           |
| ------------------- | ------------------------------------------------- | ------------------------------------- |
| **Inner Join**      | Only matching rows from both tables               | Get employees with valid departments  |
| **Cross Join**      | Cartesian product                                 | Testing, combinations                 |
| **Full Outer Join** | All rows, matched + unmatched                     | Compare two datasets                  |
| **Left Join**       | All employees + matched departments               | Employee master report                |
| **Right Join**      | All departments + matched employees               | Find empty departments                |
| **Left Semi Join**  | Employees with matching dept (only employee cols) | Filter employees present in dept list |
| **Left Anti Join**  | Employees without department match                | Find invalid / orphan records         |





# 19. Mathematical function

<img width="647" height="867" alt="Screenshot 2025-11-28 153514" src="https://github.com/user-attachments/assets/bcafc775-7454-412b-8c7e-19bd5483c29e" />





| Function       | Meaning        | Example    | Output |
| -------------- | -------------- | ---------- | ------ |
| **ABS(x)**     | Absolute value | ABS(-10)   | 10     |
| **CEIL(x)**    | Round up       | CEIL(3.4)  | 4      |
| **FLOOR(x)**   | Round down     | FLOOR(3.4) | 3      |
| **EXP(x)**     | e^x            | EXP(1)     | 2.718… |
| **LOG(x)**     | Natural log    | LOG(10)    | 2.302… |
| **POWER(x,y)** | xⁿ             | POWER(5,2) | 25     |
| **SQRT(x)**    | Square root    | SQRT(25)   | 5      |


# 20. cast
CAST() is used to convert the data type of a column into another data type

df2 = df.withColumn("age_int", col("age_str").cast("int"))
df2.show()



# 21. windows function 


Use this for all workouts:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("window").getOrCreate()

data = [
    ("IT", "Arjun", 45000),
    ("IT", "Mathes", 65000),
    ("IT", "Madhan", 65000),
    ("CSE", "Mahendra", 36000),
    ("CSE", "Boopi", 32000),
    ("EEE", "Sakthi", 87000),
    ("EEE", "Mukesh", 54000),
    ("EEE", "Pranesh", 58000)
]

cols = ["dept", "name", "salary"]

df = spark.createDataFrame(data, cols)
df.show()
```

---
`````
win = Window.partitionBy("dept").orderBy(col("salary").desc())
``````
 Window Function Workouts

## 4.1 ROW_NUMBER()

```python
df.withColumn("row_num", row_number().over(win)).show()
```

---

## 4.2 RANK()

```python
df.withColumn("rank", rank().over(win)).show()
```

---

## 4.3 DENSE_RANK()

```python
df.withColumn("dense_rank", dense_rank().over(win)).show()
```

---

## 4.4 LEAD() → Next Salary

```python
df.withColumn("next_salary", lead("salary", 1).over(win)).show()
```

---

## 4.5 LAG() → Previous Salary

```python
df.withColumn("prev_salary", lag("salary", 1).over(win)).show()
```

---



<img width="974" height="967" alt="Screenshot 2025-12-01 180932" src="https://github.com/user-attachments/assets/cd15a73a-e3e7-47f9-ab5a-ec47b1dc02fc" />



| Function     | Purpose              |
| ------------ | -------------------- |
| ROW_NUMBER() | Unique row number    |
| RANK()       | Ranking with gaps    |
| DENSE_RANK() | Ranking without gaps |
| LEAD()       | Next row value       |
| LAG()        | Previous row value   |
| SUM() OVER   | Running total        |
| AVG() OVER   | Running average      |
| MAX() OVER   | Highest per dept     |
| MIN() OVER   | Lowest per dept     




# 22. Array functions

<img width="1159" height="923" alt="Screenshot 2025-12-01 180953" src="https://github.com/user-attachments/assets/a8f39760-a1ca-4776-b4d5-c6dee33d1ad6" />


| Function             | Description                      | Example                         |
| -------------------- | -------------------------------- | ------------------------------- |
| **array()**          | Create array from values/columns | array(lit("a"), lit("b"))       |
| **array_contains()** | Check if array has a value       | array_contains(fruits,"banana") |
| **array_length()**   | Count elements                   | array_length(fruits)            |
| **array_position()** | 1-based index of value           | array_position(fruits,"banana") |
| **array_remove()**   | Remove all occurrences           | array_remove(fruits,"banana")   |




# 23. Explode function 

# . explode()

Converts each element of an array or map into separate rows.

NULL arrays are removed.

# explode_outer()

Same as explode, but retains NULL values.

Returns a single NULL row when the array is NULL.

# . posexplode_outer()

Returns both position (index) and value.

Keeps NULL arrays like explode_outer.



<img width="937" height="891" alt="image" src="https://github.com/user-attachments/assets/7f00f0de-281f-420a-ba71-8cf8cef96451" />


# 24. UDF

* "In PySpark, a UDF (User Defined Function) is a custom function written in Python when Spark’s built-in functions are not enough.
  
* We convert our Python function into a UDF so Spark can use it inside DataFrame operations."
`````
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(name):
    return name.upper()

upper_udf = udf(upper_case, StringType())

data = [(1, "alice"), (2, "bob")]
df = spark.createDataFrame(data, ["id", "name"])

df_with_udf = df.withColumn("name_upper", upper_udf("name"))
df_with_udf.show()
Output:

output
+---+-----+----------+
| id| name|name_upper|
+---+-----+----------+
|  1|alice|     ALICE|
|  2|  bob|       BOB|
+---+-----+----------+

``````

# 25. Different File Formats with Schema:

#  Schema Definition (What is Schema?)

Schema = Structure of a DataFrame

A schema defines:

Column Names

Column Data Types (String, Integer, Double, Array, Struct, etc.)

Whether column is nullable or not

# Why Schema is important?

Controls data quality

Avoids wrong data types

Makes reading files faster (no need for Spark to infer types)


from pyspark.sql.types import StructType, StructField, IntegerType, StringType
````
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

``````

# StructType
StructType = Collection of StructField objects

It acts like a container to hold the entire schema structure.

In simple words:

👉 StructType = Table structure
👉 StructField = Column definition

# StructField
StructField defines individual columns, their data type, and nullability.

# DataType
DataType specifies the type of each column (StringType, IntegerType, etc.).


# 26 . Reading Different File Formats with Schema


# Reading CSV with Schema
```

df = spark.read.csv("data.csv", header=True, inferSchema=True)
df.show()
`````
<img width="937" height="970" alt="image" src="https://github.com/user-attachments/assets/9ec146b4-9925-4d8f-b310-9723983dcfbb" />

# Reading Parquet
```
df = spark.read.parquet("path/file.parquet")
df.show()
```
<img width="1259" height="791" alt="Screenshot 2025-12-02 184234" src="https://github.com/user-attachments/assets/32ab3614-f8b9-40bb-b5d9-06af4b5a90f6" />


# Reading JSON with Schema
````
df = spark.read.json("data.json")
df.show(truncate=False)
````


<img width="1014" height="751" alt="Screenshot 2025-12-02 184301" src="https://github.com/user-attachments/assets/5da8cd47-f65b-4276-b767-db2e85207eba" />


## 27. Different File Formats with Schema

###  Write DataFrame to CSV with Schema

* Define schema using `StructType` and `StructField`.
* Create DataFrame using the schema.
* Write DataFrame to CSV using `.write.csv()`.
```` 
df.write.option("header", "true").mode("overwrite").csv("path/output.csv")
`````

###  Write DataFrame to Parquet

* Parquet automatically preserves schema.
* Use `.write.parquet()`.


```
df.write.mode("overwrite").parquet("path/output.parquet")
```

###  Write DataFrame to JSON with Schema

* Define schema and apply during read/write.
* Write JSON using `.write.json()`.

---

```
df.write.mode("overwrite").json("path/output.json")
````

## 28. Slowly Changing Dimensions (SCD)

###  SCD Type 1

* Overwrite old data with new data.
* No history maintained.
<img width="1056" height="660" alt="Screenshot 2025-12-03 173557" src="https://github.com/user-attachments/assets/f633b5ba-a097-4b4f-8844-626a2a845006" />



###  SCD Type 2

* Maintains full history of changes.
* Includes columns such as `start_date`, `end_date`, `is_current`.

<img width="567" height="827" alt="Screenshot 2025-12-03 182253" src="https://github.com/user-attachments/assets/9080a667-19be-429f-a79c-ef43184f02f6" />



###  SCD Type 3

* Maintains partial history.
* Only stores previous and current values.

---

## 29. Write Methods in PySpark

### 3.1 Overwrite

* Existing data is fully replaced.
* `.mode('overwrite')`.

````
df.write.mode("overwrite").parquet("/path/output")
`````

### Overwrite Partition

* Only specific partition data gets replaced.
* Used with `.option('partitionOverwriteMode','dynamic')`.
````
  df.write.mode("overwrite").option("replaceWhere", "year = 2023").saveAsTable("sales")
````
###  Upsert (Merge)

* Insert + Update using Delta Lake.
* Requires `.merge()`.

`````
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
`````


###  Append

* Adds new data without deleting existing.
* `.mode('append')`.
```
df.write.mode("append").json("/path/output")
---
````
