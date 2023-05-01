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

## Data Analysis on our Web Logs
Now that we have a DataFrame containing the parsed and cleaned log file as a data frame, we can perform some 
interesting exploratory data analysis (EDA) to try and get some interesting insights!

### Content Size Statistics
Let’s compute some statistics about the sizes of content being returned by the web server. 
In particular, we’d like to know what are the average, minimum, and maximum content sizes.
We can compute the statistics by calling `.describe()` on the content_size column of `logs_df.` 
The `.describe()` function returns the count, mean, stddev, min, and max of a given column.

After we apply the `.agg()` function, we call `toPandas()` to extract and convert the result into a pandas dataframe which
has better formatting on Jupyter notebooks.

### HTTP Status Code Analysis
let’s look at the status code values that appear in the log. We want to know which status code values appear in the 
data and how many times. We again start with `logs_df`, then group by the `status` column, apply the `.count()` 
aggregation function, and sort by the `status` column.

Looks like we have a total of 8 distinct HTTP status codes. Let’s take a look at their occurrences in the form of a frequency table.
Looks like status code 200 OK is the most frequent code which is a good sign that things have been working normally most of the time.
After visualizing the status code, several status codes are almost not visible due to the huge skew in the data. 
Let’s take a log transform and see if things improve.
After log transform the results definitely look good and seem to have handled the skewness, 
let’s verify this by visualizing this data which will be much better.

### Analyzing Frequent Hosts
Let’s look at hosts that have accessed the server frequently. We will try to get the count of total accesses by
each `host` and then sort by the counts and display only the top ten most frequent hosts.
It looks good but let’s inspect the blank record in row number 9 more closely.
Looks like we have some empty strings as one of the top host names! This teaches us a valuable lesson to not just
check for nulls but also potentially empty strings when data wrangling.

### Display the Top 20 Frequent EndPoints
Now, let’s visualize the number of hits to endpoints (URIs) in the log. To perform this task, we start with our 
logs_df and group by the endpointcolumn, aggregate by count, and sort in descending order like before.
Not surprisingly GIFs, the home page and some CGI scripts seem to be the most accessed assets.

### Top Ten Error Endpoints
What are the top ten endpoints requested which did not have return code 200 (HTTP Status OK)? We create a sorted list
containing the endpoints and the number of times that they were accessed with a non-200 return code and show the top ten.
Looks like GIFs (animated\static images) are failing to load the most. Do you know why? 
Well given that these logs are from 1995 and given the internet speed we had back then, which is not surprising

### Total number of Unique Hosts
What were the total number of unique hosts who visited the NASA website in these two months? We can find this out with a few transformations.

### Number of Unique Daily Hosts
For an advanced example, let’s look at a way to determine the number of unique hosts in the entire log on a day-by-day basis.
This computation will give us counts of the number of unique daily hosts.
We’d like a DataFrame sorted by increasing day of the month which includes the day of the month and the associated 
number of unique hosts for that day.
Think about the steps that you need to perform to count the number of different hosts that make requests each day. 
_Since the log only covers a single month, you can ignore the month._ You may want to use the `dayofmonthfunction` in 
the `pyspark.sql.functions` module (which we have already imported as f.
There will be one row in this DataFrame for each row in `logs_df`. Essentially, we are just transforming each row of `logs_df`
This gives us a nice dataframe showing the total number of unique hosts per day. Let’s visualize too!

### Average Number of Daily Requests per Host
In the previous example, we looked at a way to determine the number of unique hosts in the entire log on a day-by-day basis.
Let’s now try and find the average number of requests being made per Host to the NASA website per day based on our logs.
We’d like a DataFrame sorted by increasing day of the month which includes the day of the month and the associated number
of average requests made for that day per Host.

### Counting 404 Response Codes
Create a DataFrame containing only log records with a 404 status code (Not Found). 
We make sure to `cache()` the `not_found_df` dataframe as we will use it in the rest of the examples here. 
How many 404 records do you think are in the logs?

```text
Total 404 responses: 20899
```

### Listing the Top Twenty 404 Response Code Endpoints
Using the DataFrame containing only log records with a 404 response code that we cached earlier, 
we can now print out a list of the top twenty endpoints that generate the most 404 errors. Remember, top endpoints should be in sorted order.

### Listing the Top Twenty 404 Response Code Hosts
Using the DataFrame containing only log records with a 404 response code that we cached earlier, we will now can print out 
a list of the top twenty hosts that generate the most 404 errors. Remember, top hosts should be in sorted order.
Which gives us a good idea which hosts end up generating the most 404 errors for the NASA webpage.

### Visualizing 404 Errors per Day
Let’s explore our 404 records temporally (by time) now. Similar to the example showing the number of unique daily hosts,
we will break down the 404 requests by day and get the daily counts sorted by day in `errors_by_date_sorted_df`.
We can visualize the total 404 errors per day now.

### Top Three Days for 404 Errors
Based on the earlier plot, what are the top three days of the month having the most 404 errors?
We can leverage our previously created `errors_by_date_sorted_df` for this.

### Visualizing Hourly 404 Errors
Using the DataFrame not_found_df we cached earlier, we will now group and sort by hour of the day in increasing order,
to create a DataFrame containing the total number of 404 responses for HTTP requests for each hour of the day (midnight starts at 0).
Then we will build a visualization from the DataFrame.
Which Looks like total 404 errors occur the most in the afternoon and the least in the early morning.
We can now reset the maximum rows displayed by pandas to the default value since we had changed it earlier to display a limited number of rows.

## Conclusion
We took a hands-on approach to data wrangling, parsing, analysis and visualization at scale on a very common yet 
essential case-study on Log Analytics. While the data we worked on here may not really be traditionally 'Big Data' 
from a size or volume perspective, the techniques and methodologies are generic enough to scale on larger volumes of data.