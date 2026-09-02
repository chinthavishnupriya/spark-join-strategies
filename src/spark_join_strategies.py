from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


def show_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def broadcast_hash_join(spark):
    show_section("1. BROADCAST HASH JOIN")

    lookup_data = [
        ("US", "United States"),
        ("IN", "India"),
        ("UK", "United Kingdom"),
    ]
    lookup_df = spark.createDataFrame(
        lookup_data, ["CountryCode", "CountryName"]
    )

    data = [
        ("Alice", "US"),
        ("Bob", "IN"),
        ("Cathy", "UK"),
        ("David", "US"),
    ]
    large_df = spark.createDataFrame(data, ["Name", "CountryCode"])

    result = large_df.join(
        broadcast(lookup_df), "CountryCode", "inner"
    )

    print("Small lookup table:")
    lookup_df.show()
    print("Large dataset:")
    large_df.show()
    print("Broadcast Hash Join result:")
    result.show()
    print("Physical plan:")
    result.explain()


def shuffle_hash_join(spark):
    show_section("2. SHUFFLE HASH JOIN")

    data1 = [
        ("Alice", "US"),
        ("Bob", "IN"),
        ("Cathy", "UK"),
    ]
    data2 = [
        ("US", "United States"),
        ("IN", "India"),
        ("UK", "United Kingdom"),
    ]

    df1 = spark.createDataFrame(data1, ["Name", "CountryCode"])
    df2 = spark.createDataFrame(data2, ["CountryCode", "CountryName"])

    # The PDF describes this as the no-broadcast shuffle-hash case.
    # The explicit hint makes the demonstration deterministic for these tiny datasets.
    result = df1.hint("shuffle_hash").join(
        df2.hint("shuffle_hash"), "CountryCode", "inner"
    )

    print("Shuffle Hash Join result:")
    result.show()
    print("Physical plan:")
    result.explain()


def shuffle_sort_merge_join(spark):
    show_section("3. SHUFFLE SORT MERGE JOIN")

    data1 = [
        ("Alice", "US"),
        ("Bob", "IN"),
        ("Cathy", "UK"),
    ]
    data2 = [
        ("US", "United States"),
        ("IN", "India"),
        ("UK", "United Kingdom"),
    ]

    df1 = spark.createDataFrame(data1, ["Name", "CountryCode"])
    df2 = spark.createDataFrame(data2, ["CountryCode", "CountryName"])

    # The PDF demonstrates this strategy by repartitioning both datasets on the join key.
    result = df1.repartition("CountryCode").join(
        df2.repartition("CountryCode"), "CountryCode", "inner"
    )

    print("Shuffle Sort Merge Join result:")
    result.show()
    print("Physical plan:")
    result.explain()


def broadcast_nested_loop_join(spark):
    show_section("4. BROADCAST NESTED LOOP JOIN")

    data1 = [
        ("Alice", "US"),
        ("Bob", "IN"),
    ]
    data2 = [
        ("US", "United States"),
        ("IN", "India"),
    ]

    df1 = spark.createDataFrame(data1, ["Name", "CountryCode"])
    df2 = spark.createDataFrame(data2, ["CountryCode", "CountryName"])

    # A cross join has no join condition and demonstrates the nested-loop case.
    result = df1.crossJoin(df2)

    print("Broadcast Nested Loop Join / Cross Join result:")
    result.show()
    print("Physical plan:")
    result.explain()


def optimization_examples(spark):
    show_section("5. JOIN OPTIMIZATION TECHNIQUES")

    print("A. Broadcast small tables")
    print("   Use broadcast() for small lookup or dimension tables.")

    print("B. Repartition data")
    print("   Repartition large datasets by the join key to reduce shuffle overhead.")
    print("   Example: df.repartition(\"CountryCode\")")

    print("C. Sort and bucket data")
    print("   Pre-sort and bucket data on frequently used join keys.")

    print("D. Filter early")
    print("   Apply filters before the join to reduce dataset size.")

    # Small practical example combining filtering and broadcast.
    customers = spark.createDataFrame(
        [
            (1, "Alice", "US", "active"),
            (2, "Bob", "IN", "inactive"),
            (3, "Cathy", "UK", "active"),
        ],
        ["id", "Name", "CountryCode", "status"],
    )
    countries = spark.createDataFrame(
        [
            ("US", "United States"),
            ("IN", "India"),
            ("UK", "United Kingdom"),
        ],
        ["CountryCode", "CountryName"],
    )

    optimized = (
        customers.filter("status = 'active'")
        .repartition("CountryCode")
        .join(broadcast(countries), "CountryCode", "inner")
    )

    print("Practical optimized join example:")
    optimized.show()


def main():
    spark = (
        SparkSession.builder
        .appName("SparkJoinStrategies")
        .master("local[*]")
        # Disable automatic broadcast for the non-broadcast demonstrations.
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark Join Strategies Project")
    print("Spark version:", spark.version)
    print("spark.sql.autoBroadcastJoinThreshold:",
          spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    broadcast_hash_join(spark)
    shuffle_hash_join(spark)
    shuffle_sort_merge_join(spark)
    broadcast_nested_loop_join(spark)
    optimization_examples(spark)

    print("\nProject completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
