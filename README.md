# Spark Join Strategies

A practical PySpark implementation of the **Spark Join Strategies** exercise.

## Topics Covered

1. Broadcast Hash Join
2. Shuffle Hash Join
3. Shuffle Sort Merge Join
4. Broadcast Nested Loop Join
5. Join optimization techniques
   - Broadcast small tables
   - Repartition data
   - Sort and bucket data
   - Filter early

The project follows the terminology, examples, and optimization guidance in the supplied Spark Join Strategies PDF.

## Requirements

- Python 3
- PySpark
- Apache Spark 3.x recommended

Install PySpark with:

```bash
pip install pyspark
```

## Run

```bash
python3 src/spark_join_strategies.py
```

The program prints the result and physical execution plan for each join strategy.

## Important Note

The sample datasets are intentionally small because they are teaching examples. Spark can choose a different physical join operator for small data depending on configuration. The project therefore uses explicit join hints for the Shuffle Hash Join demonstration and disables automatic broadcast at the Spark-session level so the non-broadcast examples are easier to observe.

For the Shuffle Sort Merge Join example, both DataFrames are repartitioned on `CountryCode`, matching the supplied PDF's demonstration.
