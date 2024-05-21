import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace


def get_descent_df(spark):
    df = spark.createDataFrame(
        [
            ("A", "Other Asian"),
            ("B", "Black"),
            ("C", "Chinese"),
            ("D", "Cambodian"),
            ("F", "Filipino"),
            ("G", "Guamanian"),
            ("H", "Hispanic/Latin/Mexican"),
            ("I", "American Indian/Alaskan Native"),
            ("J", "Japanese"),
            ("K", "Korean"),
            ("L", "Laotian"),
            ("O", "Other"),
            ("P", "Pacific Islander"),
            ("S", "Samoan"),
            ("U", "Hawaiian"),
            ("V", "Vietnamese"),
            ("W", "White"),
            ("X", "Unknown"),
            ("Z", "Asian India"),
        ],
        schema=["Vict Descent", "Victim Descent"],
    )
    return df


def get_crimes_df(spark):
    """
    Filter rows that have Vict Descent(not null, not '-')

    LAT LON always exist

    ```
    print(
        crimes_df.filter(
            (col('LAT').isNull()) |
            (col('LON').isNull())
        ).distinct().count()
    ) # result 0
    ```
    """
    paths = [
        "hdfs://master:9000/home/user/datasets/crime_data_2010s.parquet",
        "hdfs://master:9000/home/user/datasets/crime_data_2020s.parquet",
    ]
    crimes_df = (
        spark.read.parquet(*paths)
        .filter((col("Vict Descent") != "-") & (col("Vict Descent").isNotNull()))
        # this slice [7:4] isn't python; it's overloaded
        .filter(col("DATE OCC")[7:4] == "2015")
        .select(
            col("DR_NO"),
            col("Vict Descent"),
            col("LAT"),
            col("LON"),
        )
    )
    return crimes_df


def get_income_df(spark):
    """
    - Estimated Median Income: remove '$' and ',' and cast to float
    - Zip Code: rename to Zipcode
    """
    income_df = (
        spark.read.parquet(
            "hdfs://master:9000/home/user/datasets/LA_income_2015.parquet"
        )
        .withColumn("Zip Code", col("Zip Code").cast("string"))
        .withColumn(
            "Estimated Median Income",
            regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double"),
        )
        .withColumnRenamed("Zip Code", "ZIPcode")
    )
    return income_df


def get_revgecoding_df(spark):
    """
    - The 4th column is automatically dropped by the inference step.
    As a result, (LAT, LON) pairs are unique

    - some pairs (LAT, LON) have NULL ZIPcode, but we need ZIPcode to join to income

    - some pairs (LAT, LON) have a ZIP-4 code, which according to Wikipedia
    "it includes the five digits of the ZIP Code,
    followed by a hyphen and four digits that designated a more specific location.".
    We drop these last 4 digits.
    BEWARE: THIS AFFECTS THE QUERY RESULTS
    
    """
    revgeo_df = (
        spark.read.parquet(
            "hdfs://master:9000/home/user/datasets/revgecoding.parquet"
        ).filter(col("ZIPcode").isNotNull())
        #.withColumn("ZIPcode", col("ZIPcode")[0:5]) # this affects query result
        .withColumn("ZIPcode", col("ZIPcode"))
    )
    return revgeo_df


def main(income):
    spark = SparkSession.builder.appName("read_and_print").getOrCreate()
    start_time = time.time()

    # get dataframes
    crimes_df = get_crimes_df(spark)

    revgeo_df = get_revgecoding_df(spark)

    income_df = get_income_df(spark)

    victim_descend_df = get_descent_df(spark)

    # inner join to avoid NULL ZIPcode
    crimes_join_revgeo_df = crimes_df.join(revgeo_df, ["LAT", "LON"])

    # inner join to avoid NULL Estimated Median Income
    crimes_join_revgeo_join_income_df = crimes_join_revgeo_df.join(
        income_df,
        "ZIPcode",
    )

    # sorted (zip code, income)
    # top/bot income_df zip codes may not exist in main dataset
    # use joined dataset income instead
    distinct_zip_inc = crimes_join_revgeo_join_income_df.select(
        col("ZIPcode"), col("Estimated Median Income")
    ).distinct()

    sorted_zips_by_income = (
        distinct_zip_inc.sort(col("Estimated Median Income").desc())
        if income == "top"
        else distinct_zip_inc.sort(col("Estimated Median Income").asc())
    )

    # top 3
    where_zips = [row["ZIPcode"] for row in sorted_zips_by_income.head(3)]
    result = (
        crimes_join_revgeo_join_income_df.filter(col("ZIPcode").isin(where_zips))
        .join(victim_descend_df, "Vict Descent")
        .groupBy(col("Victim Descent"))
        .count()
        .orderBy(col("count").desc())
        .select(col("Victim Descent"), col("count"))
    )

    result.show(50)
    print(f"{income} 3 income zips: {where_zips}")
    print("time elapsed", time.time() - start_time)

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: query2.py top/bot", file=sys.stderr)
        sys.exit(-1)

    _, income = sys.argv

    if income not in {"bot", "top"}:
        print("Usage: Argument must be literals: top or bot", file=sys.stderr)
        sys.exit(-1)

    main(income)
