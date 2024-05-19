from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import col, regexp_replace


def query_sql(df, spark): ...


def query_df(df): ...


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
    crimes_df = (
        spark.read.csv(
            "./data/Crime_Data_2010s.csv",
            header=True,
            inferSchema=True,
        )
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
        spark.read.csv(
            "./data/LA_income_2015.csv",
            header=True,
            inferSchema=True,
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
    - some pairs (LAT, LON) have NULL ZIPcode, but we need ZIPcode to join to income

    - some pairs (LAT, LON) have a ZIP-4 code, which according to Wikipedia
    "it includes the five digits of the ZIP Code,
    followed by a hyphen and four digits that designated a more specific location.".
    We drop these last 4 digits

    - (LAT, LON) pairs are unique

    ```
    print('yo1', revgeo_df.distinct().count()) # result 37416

    print('yo2', revgeo_df.select(col("LAT"), col("LON")).distinct().count()) # result 37416
    ```
    """
    revgeo_df = (
        spark.read.csv(
            "./data/revgecoding.csv",
            header=True,
            inferSchema=True,
        )
        .filter(col("ZIPcode").isNotNull())
        .withColumn("ZIPcode", col("ZIPcode")[0:5])
    )
    return revgeo_df


def main():
    spark = SparkSession.builder.appName("read_and_print").getOrCreate()
    start_time = time.time()

    # get dataframes
    crimes_df = get_crimes_df(spark)
    # print(f"crimes_df rows: {crimes_df.distinct().count()}")

    revgeo_df = get_revgecoding_df(spark)

    income_df = get_income_df(spark)

    victim_descend_df = get_descent_df(spark)

    # inner join to avoid NULL ZIPcode
    crimes_join_revgeo_df = crimes_df.join(revgeo_df, ["LAT", "LON"])
    # print(f"crimes_join_revgeo_df rows: {crimes_join_revgeo_df.distinct().count()}")

    # inner join to avoid NULL Estimated Median Income
    crimes_join_revgeo_join_income_df = crimes_join_revgeo_df.join(
        income_df,
        "ZIPcode",
    )
    # print(
    #    "crimes_join_revgeo_join_income_df rows: "
    #    f"{crimes_join_revgeo_join_income_df.distinct().count()}"
    # )

    # print(crimes_join_revgeo_join_income_df.take(1))

    #
    # print(crimes_join_revgeo_join_income_df.select(col('ZIPcode'), col('Estimated Median Income')).distinct().count())
    # print(crimes_join_revgeo_join_income_df.select(col('ZIPcode')).distinct().count())

    # group by (ZIPcode, Estimated Median Income, Vict Descent) and count
    zc_inc_vic_count = crimes_join_revgeo_join_income_df.groupBy(
        col("ZIPcode"), col("Estimated Median Income"), col("Vict Descent")
    ).count()

    # join with nice description
    zc_inc_vic_count = zc_inc_vic_count.join(victim_descend_df, "Vict Descent").select(
        col("ZIPcode"),
        col("Estimated Median Income"),
        col("Victim Descent"),
        col("count"),
    )
    # sort incomes found
    sorted_incomes = (
        zc_inc_vic_count.select(col("Estimated Median Income"), col("ZIPcode"))
        .distinct()
        .orderBy(col("Estimated Median Income").desc())
    )

    # top 3
    top_3_income_zip_codes = [row["ZIPcode"] for row in sorted_incomes.head(3)]
    top_3 = zc_inc_vic_count.filter(
        col("ZIPcode").isin(top_3_income_zip_codes)
    ).orderBy(col("Estimated Median Income").desc(), col("count").desc())
    top_3.show(100)
    top_3.explain()

    # bot 3
    bot_3_income_zip_codes = [row["ZIPcode"] for row in sorted_incomes.tail(3)]
    bot_3 = zc_inc_vic_count.filter(
        col("ZIPcode").isin(bot_3_income_zip_codes)
    ).orderBy(col("Estimated Median Income").desc(), col("count").desc())
    bot_3.show(100)

    print("time elapsed", time.time() - start_time)


main()
