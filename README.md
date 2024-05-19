# pyspark-big-data

The datasets:

- [Crime data 2010s](https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD)
- [Crime data 2020s](https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD)
- [LA Police Stations data](https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore)
- [Median Household Income by Zip Code (Los Angeles County) and Reverse Geocoding](http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz)

## Setup Datasets

### Download datasets
```
mkdir dataset_downloads
cd dataset_downloads
```

- Download crime data 2010s

Download file with wget

```
wget https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD --no-check-certificate
mv rows.csv\?accessType\=DOWNLOAD crime_data_2010s.csv
```

- Download crime data 2020s

Download file with wget

```
wget https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD --no-check-certificate
mv rows.csv\?accessType\=DOWNLOAD crime_data_2020s.csv
```

- Download LA Police Stations data

We visit the website https://geohub.lacity.org/datasets/lahub::lapd-police-stations/explore and download locally in csv format. Then upload to okeanos master server with scp and rename the file to something simpler

On local:
```
scp ./LAPD_Police_Stations_-3946316159051949741.csv user@83.212.80.87:~/dataset_downloads
```

On master:
```
mv LAPD_Police_Stations_-3946316159051949741.csv LAPD_Police_Stations.csv
```

- Download Median Household Income by Zip Code (Los Angeles County) and Reverse Geocoding:
Download and extract from tar. Move .csv files to top level.

```
wget http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz
tar -xzf data.tar.gz
mv ./income/*.csv ./
```

### Put datasets in Hadoop hdfs
Create HDFS datasets directory on ~/

```
hadoop fs -mkdir -p ~/datasets
```

Upload all .csv files to the hadoop directory

```
hadoop fs -put ./*.csv ~/datasets
```

Then check that the csv files are in the HDFS

```
hadoop fs -ls ~/datasets
```

![csv_files_in_hdfs](https://github.com/marvlach/pyspark-big-data/assets/59962578/7ce63062-ebb1-4e96-b817-f17daf16e22e)

### Transform csv to parquet

Accomplished by the script csv_to_parquet.py. It is called like:

```
spark-submit csv_to_parquet.py LAPD_Police_Stations.csv
```

It creates a directory in hdfs /LAPD_Police_Stations.parquet. The directory holds 2 files:
- _SUCCESS
- /part-00000-475387d6-f799-40b1-849c-6bbaae08c1bc-c000.snappy.parquet



It can be read in spark like 

```
sc.read.parquet(
    "hdfs://master:9000/home/user/datasets/LAPD_Police_Stations.parquet",
)
```

and returns a list of Row() objects

```
[Row(OBJECTID=1, DIVISION='HARBOR', LOCATION='2175 JOHN S. GIBSON BLVD.', PREC=5, x=6473747.20494418, y=1734313.75343426), ...]
```

Therefore we run the script csv_to_parquet.py for every .csv on HDFS:

```
spark-submit csv_to_parquet.py LAPD_Police_Stations.csv
spark-submit csv_to_parquet.py LA_income_2015.csv
spark-submit csv_to_parquet.py LA_income_2017.csv
spark-submit csv_to_parquet.py LA_income_2019.csv
spark-submit csv_to_parquet.py LA_income_2021.csv
spark-submit csv_to_parquet.py revgecoding.csv

spark-submit csv_to_parquet.py crime_data_2010s.csv
spark-submit csv_to_parquet.py crime_data_2020s.csv
```

![big_parquets_in_hdfs](https://github.com/marvlach/pyspark-big-data/assets/59962578/69886aa2-3710-4563-afb3-4454c64b496b)

## Query 1

To run query1.py you need to provide 2 arguments: the fileformat from which the data will be loaded from hdfs(csv or parquet) and the pyspark API used to run the query(mr or sql):

```
spark-submit query1.py csv mr
spark-submit query1.py parquet mr
spark-submit query1.py csv sql
spark-submit query1.py parquet sql
```

The results can be found in query1.txt