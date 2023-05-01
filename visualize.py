import re
import config
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, col, udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import functions as f

raw_data_files = config.RAW_DATA_FILES
spark_context = SparkContext()
sql_context = SQLContext(spark_context)
spark = SparkSession(spark_context)
base_dataframe = spark.read.text(raw_data_files)
base_dataframe_rdd = base_dataframe.rdd
sample_logs = [item['value'] for item in base_dataframe.take(15)]


def extract_hosts():
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    hosts = [re.search(host_pattern, item).group(1) if re.search(host_pattern, item) else 'no match' for item in
             sample_logs]
    return hosts, host_pattern


def extract_timestamps():
    timestamps_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    timestamps = [re.search(timestamps_pattern, item).group(1) for item in sample_logs]
    return timestamps, timestamps_pattern


def extract_method_uri_protocol():
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    method_uri_protocol = [
        re.search(method_uri_protocol_pattern, item).groups() if re.search(method_uri_protocol_pattern,
                                                                           item) else 'no match' for item in sample_logs
    ]
    return method_uri_protocol, method_uri_protocol_pattern


def extract_status():
    status_pattern = r'\s(\d{3})\s'
    status = [re.search(status_pattern, item).group(1) for item in sample_logs]
    return status, status_pattern


def extract_content_size():
    content_size_pattern = r'\s(\d+)$'
    content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]
    return content_size, content_size_pattern


def extract_logs_df():
    logs_df = base_dataframe.select(regexp_extract('value', extract_hosts()[1], 1).alias('host'),
                                    regexp_extract('value', extract_timestamps()[1], 1).alias('timestamp'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 1).alias('method'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 2).alias('endpoint'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 3).alias('protocol'),
                                    regexp_extract('value', extract_status()[1], 1).cast('integer').alias('status'),
                                    regexp_extract('value', extract_content_size()[1], 1).cast('integer').alias(
                                        'content_size'))
    # logs_df.show(10, truncate=True)
    logs_count = logs_df.count()
    logs_len = len(logs_df.columns)

    return logs_df


def find_missing_values():
    missing_values = base_dataframe.filter(base_dataframe['value'].isNull()).count()

    return missing_values


def find_bad_rows():
    logs_df = extract_logs_df()
    bad_rows_df = logs_df.filter(logs_df['host'].isNull() |
                                 logs_df['timestamp'].isNull() |
                                 logs_df['method'].isNull() |
                                 logs_df['endpoint'].isNull() |
                                 logs_df['status'].isNull() |
                                 logs_df['content_size'].isNull() |
                                 logs_df['protocol'].isNull())

    return bad_rows_df.count()


def count_null(col_names):
    return spark_sum(col(col_names).isNull().cast('integer')).alias(col_names)

    # to call this method:
    # exprs = [count_null(col_name) for col_name in logs_df.columns]


def handling_http_status_nulls():
    null_status_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s(\d{3})\s'))
    null_status_df.count()
    null_content_size_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s\d+$'))
    null_count_size = null_content_size_df.count()
    # null_status_df.show(truncate=False)

    return null_status_df


def find_pipeline_nulls():
    null_status_df = handling_http_status_nulls()
    bad_status_df = null_status_df.select(regexp_extract('value', extract_hosts()[1], 1).alias('host'),
                                          regexp_extract('value', extract_timestamps()[1], 1).alias(
                                              'timestamp'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 1).alias(
                                              'method'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 2).alias(
                                              'endpoint'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 3).alias(
                                              'protocol'),
                                          regexp_extract('value', extract_status()[1], 1).cast(
                                              'integer').alias(
                                              'status'),
                                          regexp_extract('value', extract_content_size()[1], 1).cast(
                                              'integer').alias(
                                              'content_size'))

    bad_status_df.show(truncate=False)


def drop_null_record():
    logs_df = extract_logs_df()
    logs_df = logs_df[logs_df['status'].isNotNull()]
    exprs = [count_null(col_name) for col_name in logs_df.columns]
    # logs_df.agg(*exprs).show()

    return logs_df


def fill_null_values():
    logs_df = drop_null_record()
    logs_df = logs_df.na.fill({'content_size': 0})
    exprs = [count_null(col_names=col_name) for col_name in logs_df.columns]
    # logs_df.agg(*exprs).show()

    return logs_df


def parse_clf_time(text):
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(text[7:11]),
        config.MONTH_MAP[text[3:6]],
        int(text[0:2]),
        int(text[12:14]),
        int(text[15:17]),
        int(text[18:20])
    )

udf_parse_time = udf(parse_clf_time)
logs_df = fill_null_values()
logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))
logs_df.cache()

# logs_df.show(10, truncate=True)
# print(logs_df.printSchema())

content_size_summary_df = logs_df.describe(['content_size'])
content_size_summary_df.toPandas()


def convert_pandas_dataframe():
    pandas_dataframe = (logs_df.agg(f.min(logs_df['content_size']).alias('min_content_size'),
                                    f.max(logs_df['content_size']).alias('max_content_size'),
                                    f.mean(logs_df['content_size']).alias('mean_content_size'),
                                    f.stddev(logs_df['content_size']).alias('std_content_size'),
                                    f.count(logs_df['content_size']).alias('count_content_size')).toPandas())

    return pandas_dataframe


def http_status_code_plot():
    status_freq_df = (logs_df.groupBy('status').count().sort('status').cache())
    status_freq_pd_df = (status_freq_df.toPandas().sort_values(by=['count'], ascending=False))
    sns.catplot(x='status', y='count', data=status_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])
    return status_freq_df, status_freq_pd_df


def http_status_code_details():
    status_freq_df = http_status_code_plot()[0]
    log_freq_df = status_freq_df.withColumn('log(count)', f.log(status_freq_df['count']))
    # log_freq_df.show()

    return log_freq_df


def http_status_detail_plot():
    status_freq_pd_df = http_status_code_plot()[1]
    log_freq_df = http_status_code_details()
    log_freq_pd_df = (log_freq_df.toPandas().sort_values(by=['log(count)'],ascending=False))
    sns.catplot(x='status', y='log(count)', data=log_freq_pd_df, kind='bar', order=status_freq_pd_df['status'])
    plt.show()


def analyze_frequent_hosts():
    host_sum_df = (logs_df.groupBy('host').count().sort('count', ascending=False).limit(10))
    host_sum_df.show(truncate=False)
    host_sum_pd_df = host_sum_df.toPandas()
    print(host_sum_pd_df.iloc[8]['host'])


def display_frequent_endponints():
    paths_df = (logs_df.groupBy('endpoint').count().sort('count', ascending=False).limit(20))
    paths_pd_df = paths_df.toPandas()

    return paths_pd_df


def top_ten_error_endpoints():
    not200_df = (logs_df.filter(logs_df['status'] != 200))
    error_endpoints_freq_df = (not200_df.groupBy('endpoint').count().sort('count', ascending=False).limit(10))
    error_endpoints_freq_df.show(truncate=False)


def daily_request_numbers():
    host_day_df = logs_df.select(logs_df.host, f.dayofmonth('time').alias('day'))
    host_day_distinct_df = (host_day_df.dropDuplicates())

    host_day_df.show(5, truncate=False)
    host_day_distinct_df.show(5, truncate=False)

    daily_hosts_df = (host_day_distinct_df.groupBy('day').count().select(col("day"), col("count").alias("total_hosts")))
    total_daily_reqests_df = (
        logs_df.select(f.dayofmonth("time").alias("day")).groupBy("day").count().select(col("day"), col("count").alias(
            "total_reqs"))
    )
    avg_daily_reqests_per_host_df = total_daily_reqests_df.join(daily_hosts_df, 'day')
    avg_daily_reqests_per_host_df = (avg_daily_reqests_per_host_df.withColumn('avg_reqs', col('total_reqs') / col('total_hosts')).sort("day"))
    avg_daily_reqests_per_host_df = avg_daily_reqests_per_host_df.toPandas()
    sns.catplot(x='day', y='avg_reqs', data=avg_daily_reqests_per_host_df, kind='point', height=5, aspect=1.5)
    plt.show()


def analyze_404_responses():
    not_found_df = logs_df.filter(logs_df["status"] == 404).cache()
    endpoints_404_count_df = (not_found_df.groupBy("endpoint").count().sort("count", ascending=False).limit(20))
    # endpoints_404_count_df.show(truncate=False)

    # Listing the Top Twenty 404 Response Code Hosts
    hosts_404_count_df = (not_found_df.groupBy("host").count().sort("count", ascending=False).limit(20))
    # hosts_404_count_df.show(truncate=False)

    # Visualizing 404 Errors per Day
    errors_by_date_sorted_df = (not_found_df.groupBy(f.dayofmonth('time').alias('day')).count().sort("day"))
    errors_by_date_sorted_pd_df = errors_by_date_sorted_df.toPandas()

    # visualizing 404 erros per day
    sns.catplot(x='day', y='count', data=errors_by_date_sorted_pd_df, kind='point', height=5, aspect=1.5)

    # Top Three Days for 404 Errors
    top_three_404_response = (errors_by_date_sorted_df.sort("count", ascending=False).show(3))

    # Visualizing Hourly 404 Errors
    hourly_avg_errors_sorted_df = (not_found_df.groupBy(f.hour('time').alias('hour')).count().sort('hour'))
    hourly_avg_errors_sorted_pd_df = hourly_avg_errors_sorted_df.toPandas()

    sns.catplot(x='hour', y='count', data=hourly_avg_errors_sorted_pd_df, kind='bar', height=5, aspect=1.5)
    plt.show()
