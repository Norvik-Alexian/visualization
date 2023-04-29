import re
import glob
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, col, udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import functions as F

spark_context = SparkContext()
sql_context = SQLContext(spark_context)
spark = SparkSession(spark_context)
raw_data_files = glob.glob('*.gz')

base_dataframe = spark.read.text(raw_data_files)
# base_dataframe.printSchema()
base_dataframe_rdd = base_dataframe.rdd

# base_dataframe.show(10, truncate=False)
# print(base_dataframe_rdd.take(10))
# print((base_dataframe.count(), len(base_dataframe.columns)))

sample_logs = [item['value'] for item in base_dataframe.take(15)]
# print(sample_logs)

# extracting host names
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1) if re.search(host_pattern, item) else 'no match' for item in sample_logs]

# extracting timestamps
timestamps_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(timestamps_pattern, item).group(1) for item in sample_logs]

# extracting HTTP request method, URLs and Protocols
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups() if re.search(method_uri_protocol_pattern, item) else 'no match' for item in sample_logs]

# extracting HTTP request status code
status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]

# extracting HTTP Response Content Size
content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]

# putting all the regex pattern together
logs_df = base_dataframe.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', timestamps_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))

# logs_df.show(10, truncate=True)
# print((logs_df.count(), len(logs_df.columns)))

# Finding missing values
(base_dataframe.filter(base_dataframe['value'].isNull()).count())

bad_rows_df = logs_df.filter(logs_df['host'].isNull()|
                             logs_df['timestamp'].isNull() |
                             logs_df['method'].isNull() |
                             logs_df['endpoint'].isNull() |
                             logs_df['status'].isNull() |
                             logs_df['content_size'].isNull()|
                             logs_df['protocol'].isNull())
# print(bad_rows_df.count())

def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
exprs = [count_null(col_name) for col_name in logs_df.columns]

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
# logs_df.agg(*exprs).show()

# handling nulls in HTTP status
null_status_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s(\d{3})\s'))
null_status_df.count()
# null_status_df.show(truncate=False)

# parsing the log data pipeline to find null values
bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', timestamps_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))

# bad_status_df.show(truncate=False)

# drop the record
logs_df = logs_df[logs_df['status'].isNotNull()]
exprs = [count_null(col_name) for col_name in logs_df.columns]
# logs_df.agg(*exprs).show()


# handling nulls in HTTP content size
null_content_size_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s\d+$'))
# print(null_content_size_df.count())

# print(null_content_size_df.take(10))

logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
# logs_df.agg(*exprs).show()

# Handling Temporal Fields (Timestamp)
month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )

udf_parse_time = udf(parse_clf_time)

logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))

# logs_df.show(10, truncate=True)
# print(logs_df.printSchema())
logs_df.cache()


content_size_summary_df = logs_df.describe(['content_size'])
content_size_summary_df.toPandas()

pandas_dataframe = (logs_df.agg(F.min(logs_df['content_size']).alias('min_content_size'),
                    F.max(logs_df['content_size']).alias('max_content_size'),
                    F.mean(logs_df['content_size']).alias('mean_content_size'),
                    F.stddev(logs_df['content_size']).alias('std_content_size'),
                    F.count(logs_df['content_size']).alias('count_content_size')).toPandas())


# HTTP Status Code Analysis
status_freq_df = (logs_df.groupBy('status').count().sort('status').cache())
# print('Total distinct HTTP Status Codes:', status_freq_df.count())

status_freq_pd_df = (status_freq_df.toPandas().sort_values(by=['count'],ascending=False))
sns.catplot(x='status', y='count', data=status_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])

log_freq_df = status_freq_df.withColumn('log(count)', F.log(status_freq_df['count']))
# log_freq_df.show()

log_freq_pd_df = (log_freq_df.toPandas().sort_values(by=['log(count)'],ascending=False))
sns.catplot(x='status', y='log(count)', data=log_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])

# Analyzing Frequent Hosts
host_sum_df =(logs_df.groupBy('host').count().sort('count', ascending=False).limit(10))
# host_sum_df.show(truncate=False)

# host_sum_pd_df = host_sum_df.toPandas()
# print(host_sum_pd_df.iloc[8]['host'])

# Display the Top 20 Frequent EndPoints
paths_df = (logs_df.groupBy('endpoint').count().sort('count', ascending=False).limit(20))
paths_pd_df = paths_df.toPandas()

# Top Ten Error Endpoints
not200_df = (logs_df.filter(logs_df['status'] != 200))
error_endpoints_freq_df = (not200_df.groupBy('endpoint').count().sort('count', ascending=False).limit(10))
# error_endpoints_freq_df.show(truncate=False)

# Average Number of Daily Requests per Host
host_day_df = logs_df.select(logs_df.host, F.dayofmonth('time').alias('day'))
host_day_distinct_df = (host_day_df.dropDuplicates())

# host_day_df.show(5, truncate=False)
# host_day_distinct_df.show(5, truncate=False)

daily_hosts_df = (host_day_distinct_df.groupBy('day').count().select(col("day"),col("count").alias("total_hosts")))
total_daily_reqests_df = (logs_df.select(F.dayofmonth("time").alias("day")).groupBy("day").count().select(col("day"), col("count").alias("total_reqs")))

avg_daily_reqests_per_host_df = total_daily_reqests_df.join(daily_hosts_df, 'day')
avg_daily_reqests_per_host_df = (avg_daily_reqests_per_host_df.withColumn('avg_reqs', col('total_reqs') / col('total_hosts')).sort("day"))
avg_daily_reqests_per_host_df = avg_daily_reqests_per_host_df.toPandas()

avg_daily_request_plot = sns.catplot(x='day', y='avg_reqs', data=avg_daily_reqests_per_host_df, kind='point', height=5, aspect=1.5)

# Counting 404 Response Codes
not_found_df = logs_df.filter(logs_df["status"] == 404).cache()
total_404_response = ('Total 404 responses: {}').format(not_found_df.count())
endpoints_404_count_df = (not_found_df.groupBy("endpoint").count().sort("count", ascending=False).limit(20))
# endpoints_404_count_df.show(truncate=False)

# Listing the Top Twenty 404 Response Code Hosts
hosts_404_count_df = (not_found_df.groupBy("host").count().sort("count", ascending=False).limit(20))
# hosts_404_count_df.show(truncate=False)

# Visualizing 404 Errors per Day
errors_by_date_sorted_df = (not_found_df.groupBy(F.dayofmonth('time').alias('day')).count().sort("day"))
errors_by_date_sorted_pd_df = errors_by_date_sorted_df.toPandas()

# visualizing 404 erros per day
response_404_plot = sns.catplot(x='day', y='count', data=errors_by_date_sorted_pd_df, kind='point', height=5, aspect=1.5)

# Top Three Days for 404 Errors
top_three_404_response = (errors_by_date_sorted_df.sort("count", ascending=False).show(3))

# Visualizing Hourly 404 Errors
hourly_avg_errors_sorted_df = (not_found_df.groupBy(F.hour('time').alias('hour')).count().sort('hour'))
hourly_avg_errors_sorted_pd_df = hourly_avg_errors_sorted_df.toPandas()

hourly_404_plot = sns.catplot(x='hour', y='count', data=hourly_avg_errors_sorted_pd_df, kind='bar', height=5, aspect=1.5)
# plt.show()
