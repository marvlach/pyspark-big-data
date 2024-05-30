import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, count, avg

import geopy.distance


def get_distance_diff(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


def get_crimes_df(spark):
    df1 = spark.read.parquet(
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.parquet",
    )
    df2 = spark.read.parquet(
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.parquet",
    )

    df_union = df1.union(df2)
    return df_union



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
                (col("Weapon Used Cd").isNotNull())
                & (col("Weapon Used Cd") >= 100)
                & (col("Weapon Used Cd") < 200)
                & ((col("LAT") != 0) | (col("LON") != 0))
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
        WHERE 
            crimes.`Weapon Used Cd` IS NOT NULL
            AND crimes.`Weapon Used Cd` BETWEEN 100 AND 199
            AND (crimes.`LAT` != 0 OR crimes.`LON` != 0)
    )
    GROUP BY DIVISION
    ORDER BY `COUNT` DESC
    """
    res = spark.sql(query)
    res.show(30)


def query_rdd_broadcast_join(spark, crimes_df, police_df):
    def find_in_hashmap(police_dict_broadcast, crime_row):
        key, crime_record = crime_row
        police_record = police_dict_broadcast[key]
        return crime_record + police_record

    crimes = crimes_df.rdd.filter(
        lambda row: row["Weapon Used Cd"] is not None
        and row["Weapon Used Cd"] >= 100
        and row["Weapon Used Cd"] <= 199
        and (row["LAT"] != 0 or row["LON"] != 0)
        and row["AREA "] is not None
    ).map(lambda row: (row["AREA "], (row["LAT"], row["LON"])))

    police = police_df.rdd.map(
        lambda row: (row["PREC"], (row["Y"], row["X"], row["DIVISION"]))
    )

    police_dict_broadcast = spark.sparkContext.broadcast(police.collectAsMap())
    local_police_dict = police_dict_broadcast.value
    result = (
        crimes.map(lambda row: find_in_hashmap(local_police_dict, row))
        .map(lambda r: (r[4], (get_distance_diff(r[0], r[1], r[2], r[3]), 1)))
        .reduceByKey(lambda x, y: ((x[0] + y[0]), (x[1] + y[1])))
        .map(lambda r: (r[0], r[1][0] / r[1][1], r[1][1]))
        .sortBy(lambda r: r[2], ascending=False)
    )

    print(result.take(30))


def query_rdd_repartition_join(crimes_df, police_df):

    def flatten(row):
        """
        Assuming one-to-many relationship,
        police is one of it's kind in the list.
        """
        key, records = row
        # police implicitly tagged by len of tuple
        police_record = [r for r in records if len(r) == 4][0]
        out = []
        for r in records:
            # crime implicitly tagged by len of tuple
            if len(r) == 3:
                out.append((key,) + police_record + r)
        return out

    crimes = crimes_df.rdd.filter(
        lambda row: row["Weapon Used Cd"] is not None
        and row["Weapon Used Cd"] >= 100
        and row["Weapon Used Cd"] <= 199
        and (row["LAT"] != 0 or row["LON"] != 0)
        and row["AREA "] is not None
    ).map(lambda row: (row["AREA "], (row["LAT"], row["LON"], "crimes")))

    police = police_df.rdd.map(
        lambda row: (row["PREC"], (row["Y"], row["X"], row["DIVISION"], "police"))
    )

    result = (
        crimes.union(police)
        .groupByKey()
        .flatMap(flatten)
        .map(lambda r: (r[3], (get_distance_diff(r[1], r[2], r[5], r[6]), 1)))
        .reduceByKey(lambda x, y: ((x[0] + y[0]), (x[1] + y[1])))
        .map(lambda r: (r[0], r[1][0] / r[1][1], r[1][1]))
        .sortBy(lambda r: r[2], ascending=False)
    )
    print(result.take(30))


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
    elif spark_api == "rdd_broadcast":
        query_rdd_broadcast_join(spark, crimes_df, police_df)
    else:
        query_rdd_repartition_join(crimes_df, police_df)

    print("time elapsed", time.time() - start_time)

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(
            "Usage: query2.py df/sql/rdd_broadcast/rdd_repartition",
            file=sys.stderr,
        )
        sys.exit(-1)

    _, spark_api = sys.argv

    if spark_api not in {"df", "sql", "rdd", "rdd_broadcast", "rdd_repartition"}:
        print(
            "Usage: Second argument must be literals: df/sql/rdd_broadcast/rdd_repartition",
            file=sys.stderr,
        )
        sys.exit(-1)

    main(spark_api)
