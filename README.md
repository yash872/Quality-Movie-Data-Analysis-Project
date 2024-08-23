# Quality-Movie-Data-Analysis-Project
***
## Project Overview
This project is an overview of an Weather Data Analysis Pipeline that extracts the weather data live from the weather APIs and load it into the Readshift after reuired transformation.
Process and Ingest only quality movies in Redshift Dara Warehouse using tech stack like S3, Glue Crawler, Glue Catalog, Glue Catalog Data Quality, Glue Low Code ETL, Redshift, Event Bridge, SNS, Step Functions etc

***

## Architectural Diagram
![Weather-Data-Analysis](https://github.com/yash872/Airline-Data-Ingestion-Project/blob/main/Images/Weather-Data-Analysis.jpg)

***

## Key Steps
### 1. Create a S3 bucket
- we will create a S3 bucket "movies-data-yb" with multiple folders for keeping the input data, bad data, quality check outcomes etc.
![S3](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/S3.JPG)

- Upload the movies data in input_data folder "imbd_movies_ratings.csv"
![s3Data](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/s3Data.JPG)

### 2. Create a Glue Crawler
- we will create a Glue Carwler "crawl-movies-data-s3" to crawl the input data schema from S3. 
![crawler](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/crawler.JPG)

- Run the crawler and check the result
![crawlerRun](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/crawlerRun.JPG)
![crawlerSchema](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/crawlerSchema.JPG)

### 3. Create a Data Quality Check
- we will create a Glue Data Quality Check by creating multiple rules on top of the crawled result. 
![dq](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/dq.JPG)

- Run the data quality rules and check outcome
![dqRes](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/dqRes.JPG)
 
 ### 4. Create a Data Quality Check
- we will create a Glue Data Quality Check by creating multiple rules on top of the crawled result. 
![dq](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/dq.JPG)

