import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, count, avg

import geopy.distance


def get_distance_diff(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


def get_crimes_df(spark):
    paths = [
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.parquet",
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.parquet",
    ]
    crimes_df = spark.read.parquet(*paths)
    return crimes_df


def get_police_station_df(spark):
    police_df = spark.read.parquet(
        "hdfs://master:9000/home/user/datasets/LAPD_Police_Stations.parquet"
    )
    return police_df


def query_df(crimes_df, police_df):
    distance_diff_udf = udf(get_distance_diff, "float")
    result = (
        (
            crimes_df.filter(
                (col("Weapon Used Cd") >= 100) & (col("Weapon Used Cd") < 200)
            )
            .join(police_df, crimes_df["AREA "] == police_df["PREC"])
            .select(
                col("LAT").alias("CRIME LAT"),
                col("LON").alias("CRIME LON"),
                col("X").alias("POLICE LON"),
                col("Y").alias("POLICE LAT"),
                col("DIVISION"),
            )
            .withColumn(
                "distance_diff",
                distance_diff_udf(
                    col("CRIME LAT"),
                    col("CRIME LON"),
                    col("POLICE LAT"),
                    col("POLICE LON"),
                ),
            )
            .groupBy(col("DIVISION"))
            .agg(
                avg("distance_diff").alias("AVG DIST DIFF"),
                count("distance_diff").alias("COUNT"),
            )
        )
        .select(col("DIVISION"), col("AVG DIST DIFF"), col("COUNT"))
        .orderBy(col("COUNT").desc())
    )

    result.show(30)


def query_sql(spark, crimes_df, police_df):
    crimes_df.registerTempTable("crimes")
    police_df.registerTempTable("police")
    spark.udf.register("get_distance_diff_udf", get_distance_diff, "float")
    query = """
    SELECT 
        DIVISION,
        AVG(`DIST DIFF`) AS `AVG DIST DIFF`,
        COUNT(*) AS `COUNT`
    FROM (
        SELECT 
            crimes.`LAT` AS `CRIME LAT`,
            crimes.`LON` AS `CRIME LON`,
            police.`Y` AS `POLICE LAT`,
            police.`X` AS `POLICE LON`,
            police.`DIVISION` AS `DIVISION`,
            get_distance_diff_udf(crimes.`LAT`, crimes.`LON`, police.`Y`, police.`X`) as `DIST DIFF`
        FROM crimes
        JOIN police on crimes.`AREA ` = police.`PREC`
        WHERE crimes.`Weapon Used Cd` BETWEEN 100 AND 199
    )
    GROUP BY DIVISION
    ORDER BY `COUNT` DESC
    """
    res = spark.sql(query)
    res.show(30)


def main(spark_api):
    spark = SparkSession.builder.appName(f"query 4 {spark_api}").getOrCreate()
    start_time = time.time()

    # get dataframes
    crimes_df = get_crimes_df(spark)
    police_df = get_police_station_df(spark)

    if spark_api == "df":
        query_df(crimes_df, police_df)
    elif spark_api == "sql":
        query_sql(spark, crimes_df, police_df)
    else:
        ...
    print("time elapsed", time.time() - start_time)

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(
            "Usage: query2.py df/sql/rdd",
            file=sys.stderr,
        )
        sys.exit(-1)

    _, spark_api = sys.argv

    if spark_api not in {"df", "sql", "rdd"}:
        print("Usage: Second argument must be literals: df/sql/rdd", file=sys.stderr)
        sys.exit(-1)

    main(spark_api)
