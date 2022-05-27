# Data Pipeline for [OpenWeatherAPI](openweathermap.org) Data
Following is the archtecture and steps required to build the data pipeline:

### Architecture
![architecture](/docs/images/architecture.png)

### Steps
* Configure the S3 Storage. In this example I am using only S3 storage for the entire data pipeline. We can change the storage based on the requirements
![s3](/docs/images/s3.png)
I have 3 folders for storing the data  
    * raw folder - contains the raw data fetched from the weather api via the lambda function
    * filtered folder - contains the filtered data that will be used by the aws GLUE for further processing
    * processed folder - contains the outputs in the required format

* Configure the lambda function. Lambda function can be used to fetch the current weather data from the [OpenWeatherAPI](openweathermap.org).
    * we are fetching and processing (filtering only required records for further processing) the data in a single [lambda function script](./scripts/lambda/fetchWeatherData.py)
    ![lambda-output](/docs/images/lambda-output.png)
    * A more advanced use case is we can mainitan two lambda functions, a lambda function triggers daily and stores the data in S3 Storage. whenever a data record is written into the S3 Storage we can trigger a event to process that record and store into S3 Storage or DB

* Configure the AWS GlUE Crawler. We can directly map the crawler to the data store, in this case it is S3 Storage and map to the result DB
![glue-crawler](/docs/images/glue-crawler.png)
Once we excute the crawler, we'll be getting the data processed in the data store created by crawler
![data-store](/docs/images/data-store.png)

* Use the data store as the input and process the data using the [AWS GLUE jobs script](./scripts/glue/TransformFilterUsingDataStore.py)
![glue-job](/docs/images/glue-job.png)
GLUE provides multiple ways to schedule the jobs. In this example, I have used the SQL to process the records.
Once the processing is completed, data will be finally stored back in the S3 Storage. Here in this example I have used the S3 Storage but we can use other storages based on the requirements

Note: we need to configure the appropriate IAM roles for the services being used in the above steps