from pyspark.sql import SparkSession
import sys
import time
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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


def read_from_parquet(spark):
    """
    Reads parquet chunks from 2 hdfs directories
    and returns a dataframe
    """
    df1 = spark.read.parquet(
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.parquet",
    )
    df2 = spark.read.parquet(
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.parquet",
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
    query = (
        df.rdd.map(
            lambda row: (
                (row["DATE OCC"][6:10], row["DATE OCC"][:2]),
                1,
            )
        )  # ((year, month), 1)
        .reduceByKey(lambda x, y: x + y)  # ((year, month), crimes)
        .map(lambda row: (row[0][0], (row[0][1], row[1])))  # (year, (month, crimes))
        .groupByKey()  # (year, [(month, crimes)])
        .mapValues(
            lambda months_crimes: list(
                enumerate(sorted(months_crimes, key=lambda x: x[1], reverse=True)[:3])
            )
        )  # (year, [(index, (month, crimes))])
        .sortByKey(ascending=True)  # (year, [(index, (month, crimes))])
        .flatMapValues(lambda x: x)  # (year, (index, (month, crimes)))
        .map(
            lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][0] + 1)
        )  # (year, month, crimes, index)
    )
    print(query.collect())


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
    SELECT year, month, count, rank
    FROM (
        SELECT 
            year,
            month,
            count,
            row_number() over (partition by year order by count desc) rank
            FROM (
                SELECT year, month, count(*) as count
                FROM (
                    SELECT 
                        substring(`DATE OCC`, 7, 4) as year,
                        substring(`DATE OCC`, 0,2) as month 
                    FROM crimes
                ) crimes_ym
                GROUP BY year, month
        ) ymc
    )
    WHERE rank < 4
    ORDER BY year ASC, rank ASC
    """

    res = spark.sql(query)
    res.show(50)


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
        (
            df.withColumns({"year": df["DATE OCC"][7:4], "month": df["DATE OCC"][0:2]})
            .select(col("year"), col("month"))
            .groupBy(col("year"), col("month"))
            .count()
        )
        .withColumn(
            "rank",
            row_number().over(Window.partitionBy("year").orderBy(col("count").desc())),
        )
        .filter(col("rank") < 4)
        .orderBy(col("year").asc(), col("rank").asc())
    )
    result.show(50)


def main(file_format, spark_api):
    spark = SparkSession.builder.appName(
        f"query1 {file_format} {spark_api}"
    ).getOrCreate()

    start_time = time.time()

    # get DF based on user-defined file_format
    df = read_from_csv(spark) if file_format == "csv" else read_from_parquet(spark)

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
    if len(sys.argv) != 3:
        print("Usage: query1 csv/parquet df/sql", file=sys.stderr)
        sys.exit(-1)

    _, file_format, spark_api = sys.argv
    if file_format not in {"csv", "parquet"}:
        print("Usage: First argument must be literals: csv or parquet", file=sys.stderr)
        sys.exit(-1)

    if spark_api not in {"df", "sql", "rdd"}:
        print(
            "Usage: Second argument must be literals: df or sql or rdd", file=sys.stderr
        )
        sys.exit(-1)

    main(file_format, spark_api)
