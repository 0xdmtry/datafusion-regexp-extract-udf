### Spark GroupBy Optimizations

**1\. Hash-Based Aggregation (Primary Strategy)**  
Spark uses hash-based aggregation (HashAggregateExec) as the default for GroupBy, leveraging in-memory hash maps for fast key-value aggregation. Partial aggregation reduces data before shuffling; final aggregation merges results post-shuffle.

* [https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/HashAggregateExec.scala)  
* [https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-HashAggregateExec.html](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-HashAggregateExec.html)

**2\. Sort-Based Aggregation (Fallback)**  
For high-cardinality keys or memory constraints, Spark falls back to sort-based aggregation (SortAggregateExec), sorting data by keys and aggregating sequentially, with disk spilling if needed.

* [https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/SortAggregateExec.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/SortAggregateExec.scala)  
* [https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-SortAggregateExec.html](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-SparkPlan-SortAggregateExec.html)

**3\. Tungsten Execution Engine**  
Tungsten optimizes aggregation with UnsafeRow format, off-heap memory, and code generation for CPU efficiency. Hash maps use UnsafeFixedWidthAggregationMap.

* [https://github.com/apache/spark/blob/master/sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeFixedWidthAggregationMap.java](https://github.com/apache/spark/blob/master/sql/core/src/main/java/org/apache/spark/sql/execution/UnsafeFixedWidthAggregationMap.java)  
* [https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-UnsafeFixedWidthAggregationMap.html](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-UnsafeFixedWidthAggregationMap.html)

**4\. Adaptive Query Execution (AQE)**  
AQE dynamically optimizes plans, handling data skew by splitting large partitions.

* [https://github.com/apache/spark/tree/master/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive](https://github.com/apache/spark/tree/master/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive)  
* [https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-adaptive-query-execution.html](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-adaptive-query-execution.html)

**5\. Memory Management**  
Spark spills hash tables to disk under memory pressure, ensuring robustness.

* [https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalAppendOnlyMap.scala](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/collection/ExternalAppendOnlyMap.scala)

**6\. Vectorized Execution**  
For supported types, columnar processing boosts CPU efficiency.

* [https://github.com/apache/spark/tree/master/sql/core/src/main/java/org/apache/spark/sql/execution/vectorized](https://github.com/apache/spark/tree/master/sql/core/src/main/java/org/apache/spark/sql/execution/vectorized) 