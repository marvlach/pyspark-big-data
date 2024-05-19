from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad, when, col
import sys
import time


def read_from_csv(spark):
    """
    Reads 2 csvs into dataframes, unions them
    and returns one dataframe
    """
    df1 = spark.read.csv(
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.csv",
        header=True,
        inferSchema=True,
    )
    df2 = spark.read.csv(
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.csv",
        header=True,
        inferSchema=True,
    )

    df_union = df1.union(df2)
    return df_union


def query_rdd(df):
    """
    Turns the Dataframe into an RDD
    and performs Map-Reduce-like operations

    Args:
        - df: a spark Dataframe

    Prints:
        The result

    """

    def bin_it(x):
        if "0500" <= x <= "1159":
            return ("MORNING: 0500 - 1159", 1)
        elif "1200" <= x <= "1659":
            return ("AFTERNOON: 1200 - 1659", 1)
        elif "1700" <= x <= "2059":
            return ("EVENING: 1700 - 2059", 1)
        else:
            return ("NIGHT: 2100 - 0459", 1)

    result = (
        df.rdd.filter(lambda row: row["Premis Desc"] == "STREET")
        .map(lambda row: str(row["TIME OCC"]).zfill(4))
        .map(bin_it)
        .reduceByKey(lambda x, y: x + y)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print(result.collect())


def query_sql(df, spark):
    """
    Takes a Dataframe, register a SQL-like table from it
    and executes the query in raw SQL

    Args:
        - df: a spark Dataframe
        - spark: spark session

    Prints:
        The result
    """
    df.registerTempTable("crimes")
    query = """
    select time_period, count(*) as count
    from (
        select 
            LPAD(CAST(`TIME OCC` AS STRING), 4, '0') as time, 
            CASE 
                WHEN time >= '0500' AND time <= '1159' THEN 'MORNING: 0500 - 1159'
                WHEN time >= '1200' AND time <= '1659' THEN 'AFTERNOON: 1200 - 1659'
                WHEN time >= '1700' AND time <= '2059' THEN 'EVENING: 1700 - 2059'
                ELSE 'NIGHT: 2100 - 0459'
            END AS time_period
        from crimes
        where `Premis Desc` = 'STREET'
    ) tp
    GROUP BY time_period
    ORDER BY count DESC
    """

    res = spark.sql(query)
    res.show(20)


def query_df(df):
    """
    Takes a Dataframe and executes the query with
    Dataframe-ORM-like API. Definetely the worst of the bunch.

    Args:
        - df: a spark Dataframe

    Prints:
        The result
    """
    result = (
        df.filter(col("Premis Desc") == "STREET")
        .withColumn("time", col("TIME OCC").cast("string").alias("time"))
        .select(lpad(col("time"), 4, "0").alias("time"))
        .withColumn(
            "time_period",
            when(
                ("0500" <= col("time")) & (col("time") <= "1159"),
                "MORNING: 0500 - 1159",
            )
            .when(
                ("1200" <= col("time")) & (col("time") <= "1659"),
                "AFTERNOON: 1200 - 1659",
            )
            .when(
                ("1700" <= col("time")) & (col("time") <= "2059"),
                "EVENING: 1700 - 2059",
            )
            .otherwise("NIGHT: 2100 - 0459"),
        )
        .groupBy(col("time_period"))
        .count()
        .orderBy(col("count").desc())
    )

    result.show(20)


def main(spark_api):
    spark = SparkSession.builder.appName(f"query2 {spark_api}").getOrCreate()

    start_time = time.time()

    # get DF
    df = read_from_csv(spark)

    # query based on user-defined spark api
    if spark_api == "rdd":
        query_rdd(df)
    elif spark_api == "sql":
        query_sql(df, spark)
    else:
        query_df(df)

    print("time elapsed", time.time() - start_time)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: query2.py df/sql/rdd", file=sys.stderr)
        sys.exit(-1)

    _, spark_api = sys.argv

    if spark_api not in {"df", "sql", "rdd"}:
        print("Usage: Argument must be literals: df or sql or rdd", file=sys.stderr)
        sys.exit(-1)

    main(spark_api)
