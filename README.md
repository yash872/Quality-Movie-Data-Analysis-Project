# Quality-Movie-Data-Analysis-Project
***
## Project Overview
This project is an overview of a Quality Movies Data Analysis Pipeline that takes the imbd movies data from S3 and perform qualaity check and based on that bad records stored in a specific S3 folder for review and Passed data load in Redshift Dara Warehouse for futher analysis.
Tech stacks used like S3, Glue Crawler, Glue Catalog, Glue Catalog Data Quality, Glue Low Code ETL, Redshift, Event Bridge, SNS, Step Functions etc

***

## Architectural Diagram
![Quality-Movies-Data-Analysis-Project](https://github.com/yash872/Airline-Data-Ingestion-Project/blob/main/Images/Quality-Movies-Data-Analysis-Project.jpg)

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
 
### 4. Create redshift output table & Glue Crawler
- we will create a output table "imdb_movies_rating" in redshift.
![redshiftTable](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/redshiftTable.JPG)

- create a glue crawler for the ouput table
![redshiftCrawler](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/redshiftCrawler.JPG)

### 5. Create a Glue Job
- we will create a Glue ETL Job "Movies-Data-Analysis" to do below tasks
![GlueETL](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/GlueETL.JPG)

    - Perform the data check on the data with defined rules
    - load failed data in bad-records S3 bucket for review
    - load the Data check rule outcome in S3
    - load the successfully passed data in Redshift table.

### 6. Create a Event Bridge Rule
- we will create Event Rule to Trigger with the Data Quality check execution and send the output to the SNS Topic.
![eventRule](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/eventRule.JPG)


### 7. Test your Glue ETL Job
NOTE: before the GLue Job Run you should have the S3, Glue and Cloundwatch monitoring Endpoints created in your VPC
![Endpoints](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/Endpoints.JPG)

- Glue job run sccessfully 
![glueSuccess](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/glueSuccess.JPG)

- Failed data loaded inside the "bad_records" S3 bucket folder
![badRec](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/badRec.JPG)

- Rule outcome loaded in "rule_outcome_from_etl" S3 bucket folder
![RuleOutcome](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/RuleOutcome.JPG)

- Final passed data loaded in Redshift "imdb_movies_rating" Table
![redshiftData](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/redshiftData.JPG)

### 8. Create a State Machine
- we will create a State Machine "Movies-Data-Pipeline" using Step Function service.
This machine will run the crawler and execute the Glue Job with SNS notification on success and failure.
![StateMachine](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/StateMachine.JPG)

### 9. Create a Event Bridge Rule
- we will create a Event Rule "movies-data-pipeline-trigger" to trigger the State Machine on the csv file creation in S3 bucket.
![EventRuleforPipeline](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/EventRuleforPipeline.JPG)

### 10. Run Final End to End Pipeline
- we will upload the imbd date csv file in the input folder of S3 bucket, and the Process should be triggered.
- Successful Run of the State Machine
![stateRun](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/stateRun.JPG)

- Success Notification on subscribed Email ID
![EmailNoti](https://github.com/yash872/Quality-Movie-Data-Analysis-Project/blob/main/Images/EmailNoti.JPG)
