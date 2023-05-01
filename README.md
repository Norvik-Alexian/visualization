# Introduction

One of the most popular and effective enterprise case-studies which leverage analytics today is log analytics. 
Almost every small and big organization today have multiple systems and infrastructure running day in and day out. 
To effectively keep their business running, organizations need to know if their infrastructure is performing to its maximum potential. 
This involves analyzing system and application logs and maybe even apply predictive analytics on log data. 
The amount of log data is typically massive, depending on the type of organizational infrastructure and applications running on it. 
Gone are the days when we were limited by just trying to analyze a sample of data on a single machine due to compute constraints.
big data processing and open-source analytics frameworks like Spark, we can perform scalable log analytics on 
potentially millions and billions of log messages daily. The intent of this case-study oriented tutorial is to take a 
hands-on approach to showcasing how we can leverage Spark to perform log analytics at scale on semi-structured log data.

Like we mentioned before, Apache Spark is an excellent and ideal open-source framework for wrangling, 
analyzing and modeling on structured and unstructured data. Typically, server logs are a very common data source in 
enterprises and often contain a gold mine of actionable insights and information. 
Log data comes from many sources in an enterprise, such as the web, client and compute servers, 
applications, user-generated content, flat files. They can be used for monitoring servers, improving business and 
customer intelligence, building recommendation systems, fraud detection, and much more.

Spark allows you to dump and store your logs in files on disk cheaply, while still providing rich APIs to perform data analysis at scale.
In this project we will analyze log datasets from NASA Kennedy Space Center web server in Florida.

We have 2 datasets that contain two months’ worth of all HTTP requests to the NASA Kennedy Space Center WWW server in Florida.

## Setting up Dependencies
If we don't have variables pre-configured, we can load them up and configure them using the following code. 
Besides this we also load up some other libraries for working with dataframes and regular expressions.

Working with regular expressions will be one of the major aspects of parsing log files. 
Regular expressions are a really powerful pattern matching technique which can be used to extract and find patterns
in semi-structured and unstructured data.

## Loading and Viewing the NASA Log Dataset
Given that our data is stored, let’s load it into a DataFrame. We’ll do this in steps. Let's load the log data file names in our disk.
Now, we’ll use `sqlContext.read.text()` or `spark.read.text()` to read the text file. 
This will produce a DataFrame with a single string column called value.
This allows us to see the schema for our log data which apparently looks like text data which we shall inspect soon. 
We can view the type of data structure holding our log data too.

After load the data, now we can take a peek at the actual log data in our dataframe which definitely looks like 
standard server log data which is semi-structured, and we will definitely need to do some data processing and 
wrangling before this can be useful.

## Data Wrangling
In Dataa wrangling section of the project we will try and clean and parse our log dataset to really extract structured
attributes with meaningful information from each log message.
We will need to use some specific techniques to parse, match and extract these attributes from the log data.

### Data Parsing and Extraction with Regular Expressions
we have to parse our semi-structured log data into individual columns. 
We’ll use the special built-in `regexp_extract()` function to do the parsing. 
This function matches a column against a regular expression with one or more capture groups and allows you to extract
one of the matched groups. We’ll use one regular expression for each field we wish to extract.

Looks like we have a total of approximately 3.46 million log messages. Not a small number! Let’s extract and take a 
look at some sample log messages.

### Extracting the necessary data
We need to write a regular expression to extract a couple of meaningful data from our logs:
1. Host names
2. Timestampts
3. HTTP request methods, URIs and Protocol
4. HTTP status code
5. HTTP response content size

And pull it all together into a pipeline.

## Finding Missing Values
Missing and null values are the bane of data analysis and machine learning. 
Let’s see how well our data parsing and extraction logic worked. To do that First, let’s verify that there are no null 
rows in the original dataframe which shows 0 and that is a good news! And if our data parsing and extraction worked properly, 
we should not have any rows with potential null values, but unfortunately we have more than 33K missing values in our data.

Do remember, this is not a regular pandas dataframe which you can directly query and get which columns have null. 
Our so-called big dataset is residing on disk which can potentially be present in multiple nodes in a spark cluster.

### Finding Null counts
We can use a technique to find ut which columns have null values. It looks like we have missing values in `status` and
`content_size` column.
We will create a regex pattern and pass it to the pipeline that we created earlier to see the bad records in the data,
and it looks like incomplete record with no useful information, the best option would be to drop this record.

### Handling nulls in HTTP content size
First we should look at the top ten records of our data frame having missing content sizes. And It is quite evident 
that the bad raw data records correspond to error responses, where no content was sent back and the server emitted a
"-" for the content_size field. Since we don’t want to discard those rows from our analysis, let’s impute or fill them to 0.

### Fix the rows with null content_size
The easiest solution is to replace the null values in logs_df with 0 like we discussed earlier. 
The Spark DataFrame API provides a set of functions and fields specifically designed for working with null values, 
among them:
* `fillna()`, which fills null values with specified non-null values.
* `na`, which returns a `DataFrameNaFunctions` object with many functions for operating on null columns.

There are several ways to invoke this function. The easiest is just to replace all null columns with known values.
But, for safety, it’s better to pass a Python dictionary containing (column_name, value) mappings. That’s what we’ll do.

### Handling Temporal Fields (Timestamp)
Now that we have a clean, parsed DataFrame, we have to parse the timestamp field into an actual timestamp. 
The Common Log Format time is somewhat non-standard. A User-Defined Function (UDF) is the most straightforward way to parse it.
