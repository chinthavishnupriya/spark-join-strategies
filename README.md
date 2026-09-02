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

- Python 3.14 tested
- PySpark 4.2.0 tested
- Apache Spark 4.2.0 tested
- Java compatible with the installed Spark distribution

Install dependencies with:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python src/spark_join_strategies.py
```

The program prints the input data where useful, the result, and the physical execution plan for each join strategy.

## Verified Execution

The project was successfully executed with **Python 3.14.4** and **PySpark 4.2.0**.

The physical plans confirmed these operators:

| Demonstration | Physical operator |
|---|---|
| Broadcast Hash Join | `BroadcastHashJoin` |
| Shuffle Hash Join | `ShuffledHashJoin` |
| Shuffle Sort Merge Join | `SortMergeJoin` |
| Broadcast Nested Loop / Cross Join | `CartesianProduct` |

The execution finished with:

```text
Project completed successfully.
```

## Important Note

The sample datasets are intentionally small because they are teaching examples. Spark can choose a different physical join operator for small data depending on configuration. The project therefore uses an explicit `shuffle_hash` join hint for the Shuffle Hash Join demonstration and disables automatic broadcast at the Spark-session level so the non-broadcast examples are easier to observe.

For the Shuffle Sort Merge Join example, both DataFrames are repartitioned on `CountryCode`, matching the supplied PDF's demonstration.

The Broadcast Nested Loop Join section uses `crossJoin()`. Spark's physical plan for this demonstration appears as `CartesianProduct`, which is the expected physical representation of the cross join in the tested execution.

## Project Structure

```text
spark-join-strategies/
├── .gitignore
├── README.md
├── requirements.txt
└── src/
    └── spark_join_strategies.py
```
