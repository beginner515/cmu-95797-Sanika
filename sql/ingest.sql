-- ingest data from the various files into duckdb

-- from .parquet files

CREATE TABLE fhv_tripdata AS SELECT * FROM read_parquet('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\taxi\fhv_tripdata.parquet', union_by_name=True, filename=True);

CREATE TABLE fhvhv_tripdata AS SELECT * FROM read_parquet('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\taxi\fhvhv_tripdata.parquet', union_by_name=True, filename=True);

CREATE TABLE green_tripdata AS SELECT * FROM read_parquet('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\taxi\green_tripdata.parquet', union_by_name=True, filename=True);

CREATE TABLE yellow_tripdata AS SELECT * FROM read_parquet('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\taxi\yellow_tripdata.parquet', union_by_name=True, filename=True);

-- from .csv files

CREATE TABLE fhv_bases AS SELECT * FROM read_csv_auto('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\fhv_bases.csv', HEADER=True, union_by_name=True, filename=True, all_varchar=1);

CREATE TABLE central_park_weather AS SELECT * FROM read_csv_auto('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\central_park_weather.csv', HEADER=True, union_by_name=True, filename=True, all_varchar=1);


-- from compressed .csv files

CREATE TABLE bike_data AS SELECT * FROM read_csv_auto('C:\Users\Sanika\Softwares\Application_checklist\CMU\Data Warehousing\cmu-95797-Sanika\data\citibike-tripdata.csv.gz', HEADER=True, union_by_name=True, filename=True, all_varchar=1);