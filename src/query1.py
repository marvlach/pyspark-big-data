from pyspark.sql import SparkSession
import sys
import time


def read_from_csv(spark):

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
    paths = [
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.parquet",
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.parquet",
    ]
    df = spark.read.parquet(*paths)
    return df


def query_mr(df):
    query = (
        df.rdd.map(
            lambda row: (
                (row["Date Rptd"][6:10], row["Date Rptd"][:2]),
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
    df.registerTempTable("crimes")
    query = """
    select month, year, count, rank
    from (
        select 
            month,
            year,
            count,
            row_number() over (partition by year order by count desc) rank
            from (
                SELECT month, year, count(*) as count
                from (
                    SELECT 
                        substring(`Date Rptd`, 7, 4) as year,
                        substring(`Date Rptd`, 0,2) as month 
                    FROM crimes
                ) crimes_ym
                GROUP BY year, month
        ) ymc
    )
    where rank < 4
    order by year ASC, rank ASC
    """

    res = spark.sql(query)
    res.show(50)


def main(file_format, spark_api):
    spark = SparkSession.builder.appName(
        f"query1 {file_format} {spark_api}"
    ).getOrCreate()

    start_time = time.time()

    # get DF based on user-defined file_format
    df = read_from_csv(spark) if file_format == "csv" else read_from_parquet(spark)

    # query based on user-defined spark api
    if spark_api == "mr":
        query_mr(df)
    else:
        query_sql(df, spark)

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

    if spark_api not in {"mr", "sql"}:
        print("Usage: Second argument must be literals: mr or sql", file=sys.stderr)
        sys.exit(-1)

    main(file_format, spark_api)
