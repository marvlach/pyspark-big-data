from pyspark.sql import SparkSession
import sys

def main(source_csv_file_name):
    sc = SparkSession.builder.appName("HDFS csv to parquet").getOrCreate()

    df = sc.read.csv(
        f"hdfs://master:9000/home/user/datasets/{source_csv_file_name}.csv",
        header=True,
        inferSchema=True,
    )

    df.write.parquet(f"hdfs://master:9000/home/user/datasets/{source_csv_file_name}.parquet")
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)
    source_csv_file_name = sys.argv[1]
    source_csv_file_name = source_csv_file_name[:-4]
    main(source_csv_file_name)